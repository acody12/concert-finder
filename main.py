"""
AUTOMATED CONCERT DISCOVERY AND PLAYLIST CREATOR
Scrapes concerts, finds Spotify tracks, creates playlists automatically
Shows all artists; adds fuzzy match details only for non-exact matches
"""
import spotipy
import os
from spotipy.oauth2 import SpotifyOAuth
import json
import requests
import pandas as pd
import time
import re
import math
import pytz
import base64
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
from difflib import SequenceMatcher

class Config:
    CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
    CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET") 
    REDIRECT_URI = os.getenv("SPOTIPY_REDIRECT_URI")
    SCOPE = """user-read-private user-read-email playlist-read-private 
               playlist-read-collaborative playlist-modify-public playlist-modify-private"""
    CACHE_PATH = ".spotify_cache"
    
    LAT = 42.004414
    LON = -87.671304

    DAYS_IN_FUTURE = 0

    AUTO_ADD_THRESHOLD = 0.85
    SKIP_THRESHOLD = 0.70
    
    FESTIVAL_PERFORMER_LIMIT = 6

class Utils:
    @staticmethod
    def calculate_distance(lat1, lon1, lat2, lon2):
        if not all([lat1, lon1, lat2, lon2]):
            return float('inf')
        try:
            lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
            coords = [math.radians(x) for x in [lat1, lon1, lat2, lon2]]
            lat1, lon1, lat2, lon2 = coords
            dlat, dlon = lat2 - lat1, lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            return round(2 * 3956 * math.asin(math.sqrt(a)), 2)
        except Exception:
            return float('inf')
    
    @staticmethod
    def clean_artist_name(name):
        return re.sub(r'\s*\([A-Z]{2,3}\)\s*$', '', name.strip())
    
    @staticmethod
    def similarity_score(a, b):
        return SequenceMatcher(None, a.lower(), b.lower()).ratio()
    
    @staticmethod
    def has_region_identifier(name):
        return bool(re.search(r'\s*\([A-Z]{2,3}\)\s*$', name))

class SpotifyClient:
    def __init__(self):
        self.sp = None
        self.access_token = None
    
    def authenticate(self):
        print("Authenticating with Spotify...")
        sp_auth = SpotifyOAuth(
            Config.CLIENT_ID, Config.CLIENT_SECRET, Config.REDIRECT_URI,
            scope=Config.SCOPE, cache_path=Config.CACHE_PATH
        )
        
        token_info = sp_auth.get_cached_token()
        if not token_info:
            auth_url = sp_auth.get_authorize_url()
            print(f"Go here:\n{auth_url}")
            redirected_url = input("Paste redirected URL: ")
            try:
                code = sp_auth.parse_response_code(redirected_url)
                token_info = sp_auth.get_access_token(code)
            except Exception as e:
                print(f"Error parsing redirect URL: {e}")
                return False
        
        if token_info and 'access_token' in token_info:
            self.sp = spotipy.Spotify(auth=token_info['access_token'])
            try:
                user = self.sp.current_user()
                print(f"Logged in as: {user['display_name']}")
                return True
            except Exception as e:
                print(f"Error getting user info: {e}")
                return False
        
        print("Login failed.")
        return False
    
    def get_search_token(self):
        auth_string = f"{Config.CLIENT_ID}:{Config.CLIENT_SECRET}"
        b64_auth = base64.b64encode(auth_string.encode()).decode()
        
        try:
            response = requests.post(
                'https://accounts.spotify.com/api/token',
                headers={
                    'Authorization': f'Basic {b64_auth}',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                data={'grant_type': 'client_credentials'}
            )
            response.raise_for_status()
            self.access_token = response.json()['access_token']
            return True
        except Exception as e:
            print(f"Error getting search token: {e}")
            return False
    
    def search_artist(self, artist_name):
        if not self.access_token:
            return {'searched_artist': artist_name, 'error': 'No access token'}
        
        headers = {'Authorization': f'Bearer {self.access_token}'}
        search_name = Utils.clean_artist_name(artist_name)
        has_region = Utils.has_region_identifier(artist_name)
        
        try:
            response = requests.get(
                "https://api.spotify.com/v1/search",
                headers=headers,
                params={'q': search_name, 'type': 'artist', 'limit': 50}
            )
            response.raise_for_status()
            response_data = response.json()
            
            if 'artists' not in response_data:
                return {'searched_artist': artist_name, 'error': f'API Error: {response_data}'}
            
            artists = response_data['artists']['items']
            if not artists:
                return {'searched_artist': artist_name, 'error': 'No artist found'}
            
            chosen_artist, exact_match, similarity = self._find_best_match(
                artists, artist_name, search_name, has_region
            )
            
            if not chosen_artist:
                return {'searched_artist': artist_name, 'error': 'No suitable artist found'}
            
            track, _ = self._get_artist_track(chosen_artist['id'], headers)
            if not track:
                return {
                    'searched_artist': artist_name, 
                    'found_artist': chosen_artist['name'], 
                    'error': 'No tracks found'
                }
            
            return {
                'searched_artist': artist_name,
                'found_artist': chosen_artist['name'],
                'uri': track['uri'],
                'exact_match': exact_match,
                'similarity_score': similarity,
                'had_region_identifier': has_region
            }
        except Exception as e:
            return {'searched_artist': artist_name, 'error': f'Search error: {str(e)}'}
    
    def _find_best_match(self, artists, original_name, search_name, has_region):
        exact_matches = []
        approximate_matches = []
        
        for artist in artists:
            spotify_name = artist['name']
            
            if has_region:
                if Utils.clean_artist_name(spotify_name).lower() == search_name.lower():
                    exact_matches.append(artist)
                else:
                    sim = Utils.similarity_score(search_name, Utils.clean_artist_name(spotify_name))
                    if sim > 0.8:
                        approximate_matches.append((artist, sim))
            else:
                if spotify_name.lower() == original_name.lower():
                    exact_matches.append(artist)
                else:
                    sim = Utils.similarity_score(original_name, spotify_name)
                    if sim > 0.6:
                        approximate_matches.append((artist, sim))
        
        if exact_matches:
            best = max(exact_matches, key=lambda x: x['followers']['total'])
            return best, True, 1.0
        elif approximate_matches:
            best_match = max(approximate_matches, key=lambda x: x[1])
            return best_match[0], False, best_match[1]
        else:
            return None, False, 0.0
    
    def _get_artist_track(self, artist_id, headers):
        try:
            response = requests.get(
                f"https://api.spotify.com/v1/artists/{artist_id}/top-tracks",
                headers=headers, params={'market': 'US'}
            )
            response.raise_for_status()
            response_data = response.json()
            if response_data.get('tracks'):
                return response_data['tracks'][0], False
        except Exception:
            pass
        
        try:
            albums_response = requests.get(
                f"https://api.spotify.com/v1/artists/{artist_id}/albums",
                headers=headers, params={'limit': 1}
            )
            albums_response.raise_for_status()
            albums_data = albums_response.json()
            if albums_data.get('items'):
                album_id = albums_data['items'][0]['id']
                tracks_response = requests.get(
                    f"https://api.spotify.com/v1/albums/{album_id}/tracks",
                    headers=headers, params={'limit': 1}
                )
                tracks_response.raise_for_status()
                tracks_data = tracks_response.json()
                if tracks_data.get('items'):
                    track_id = tracks_data['items'][0]['id']
                    track_response = requests.get(
                        f"https://api.spotify.com/v1/tracks/{track_id}",
                        headers=headers
                    )
                    track_response.raise_for_status()
                    return track_response.json(), True
        except Exception:
            pass
        
        return None, True
    
    def create_playlist(self, results, month, day, city, scraped_url=None):
        if not self.sp:
            return
        
        try:
            user_id = self.sp.current_user()['id']
            playlist_name = f"{month.lstrip('0')}/{day.lstrip('0')} {city} Shows"
            print(f"Debug - scraped_url: {scraped_url}")
            description = f"Concert recommendations in {city}, sorted by proximity. Data from: {scraped_url}"
            print(f"Creating playlist: '{playlist_name}'")
            
            playlist_id = self._get_or_create_playlist(user_id, playlist_name, description)
        
            approved_uris = []
            stats = {'exact': 0, 'fuzzy': 0, 'skipped': 0}
            
            def get_distance(r):
                dist = r.get('distance_miles', float('inf'))
                try:
                    return float(dist) if pd.notna(dist) else float('inf')
                except (TypeError, ValueError):
                    return float('inf')
            
            results_sorted = sorted(results, key=get_distance)
            
            # First: print all artists in order
            print("\n--- UPCOMING SHOWS (BY PROXIMITY) ---")
            display_num = 1
            for result in results_sorted:
                if 'error' in result:
                    continue
                
                similarity = result['similarity_score']
                if similarity < Config.SKIP_THRESHOLD:
                    continue
                
                dist = result.get('distance_miles', 'Unknown')
                venue = result.get('venue', 'Unknown Venue')
                artist = result['found_artist']  # Use the one we found on Spotify
                print(f"{display_num:2}. {artist} @ {venue} ({dist} mi)")
                display_num += 1
            
            # Then: show details only for fuzzy matches
            print("\n--- FUZZY MATCH DETAILS ---")
            has_fuzzy = False
            for result in results_sorted:
                if 'error' in result:
                    continue
                
                similarity = result['similarity_score']
                if similarity < Config.SKIP_THRESHOLD or result['exact_match']:
                    continue
                
                has_fuzzy = True
                orig = result['searched_artist']
                found = result['found_artist']
                dist = result.get('distance_miles', 'Unknown')
                venue = result.get('venue', 'Unknown Venue')
                print(f"  → '{orig}' → '{found}' @ {venue} ({dist} mi) [{similarity:.0%}]")
            
            if not has_fuzzy:
                print("  (All matches were exact)")
            
            # Now build playlist with all valid tracks (in distance order)
            for result in results_sorted:
                if 'error' in result:
                    continue
                similarity = result['similarity_score']
                if similarity >= Config.SKIP_THRESHOLD:
                    approved_uris.append(result['uri'])
                    stats['exact' if result['exact_match'] else 'fuzzy'] += 1
                else:
                    stats['skipped'] += 1
            
            if approved_uris:
                self._add_tracks_to_playlist(playlist_id, approved_uris)
                self._print_playlist_summary(playlist_name, len(approved_uris), stats, playlist_id)
            else:
                print("No tracks were approved for the playlist")
        except Exception as e:
            print(f"Error creating playlist: {e}")
    
    def _get_or_create_playlist(self, user_id, name, description):
        try:
            playlists = self.sp.current_user_playlists()['items']
            for playlist in playlists:
                if playlist['name'] == name:
                    existing = self.sp.playlist_items(playlist['id'])
                    uris_to_remove = [item['track']['uri'] for item in existing['items'] if item['track']]
                    if uris_to_remove:
                        self.sp.playlist_remove_all_occurrences_of_items(playlist['id'], uris_to_remove)
                        print(f"Removed {len(uris_to_remove)} existing tracks")
                    return playlist['id']
            new_playlist = self.sp.user_playlist_create(user_id, name, description=description, public=False)
            return new_playlist['id']
        except Exception as e:
            print(f"Error getting or creating playlist: {e}")
            raise
    
    def _add_tracks_to_playlist(self, playlist_id, track_uris):
        try:
            batch_size = 100
            for i in range(0, len(track_uris), batch_size):
                batch = track_uris[i:i + batch_size]
                self.sp.playlist_add_items(playlist_id, batch)
        except Exception as e:
            print(f"Error adding tracks to playlist: {e}")
            raise
    
    def _print_playlist_summary(self, name, track_count, stats, playlist_id):
        try:
            print(f"\nSuccessfully created playlist '{name}' with {track_count} tracks!")
            print(f"Playlist URL: {self.sp.playlist(playlist_id)['external_urls']['spotify']}")
        except Exception as e:
            print(f"Error printing playlist summary: {e}")

class ConcertScraper:
    def __init__(self, lat, lon):
        self.lat = lat
        self.lon = lon
        self.chicago_tz = pytz.timezone('America/Chicago')
    
    def get_target_date(self, days_ahead=0):
        """Get the target date in Chicago timezone"""
        return datetime.now(self.chicago_tz) + timedelta(days=days_ahead)
    
    def scrape_concerts(self, days_ahead=0):
        try:
            target_date = self.get_target_date(days_ahead)
            original_url = self._build_songkick_url(target_date, 0)  # 0 because we already added days_ahead
            url = original_url  # Keep track of original URL for playlist description
            city_match = re.search(r'us-(\w+)', url)
            city = city_match.group(1).title() if city_match else "Chicago"
            
            # Updated this line to show the actual date instead of "in X days"
            date_str = target_date.strftime("%-m/%-d") if days_ahead != 0 else "tonight"
            print(f"Scraping concerts from {city} for {date_str}...")
            print(f"URL: {url}")
            
            performers = []
            event_count = 0
            festival_count = 0
            
            while url:
                performers_batch, events, festivals, soup = self._scrape_page(url)
                performers.extend(performers_batch)
                event_count += events
                festival_count += festivals
                url = self._get_next_page_url(soup)
                time.sleep(1)
            
            print(f"Found {event_count} events, filtered out {festival_count} festivals")
            print(f"Processing {len(performers)} performers")
            
            # Pass days_ahead to _process_performers and return original_url
            return self._process_performers(performers, days_ahead), city, original_url
        except Exception as e:
            print(f"Error scraping concerts: {e}")
            return [], "Chicago", None
    
    def _build_songkick_url(self, date, days_ahead=0):
        target_date = date + timedelta(days=days_ahead)
        day, month, year = target_date.strftime("%d"), target_date.strftime("%m"), target_date.strftime("%Y")
        base_url = "https://www.songkick.com/metro-areas/9426-us-chicago"
        params = f"filters%5BmaxDate%5D={month}%2F{day}%2F{year}&filters%5BminDate%5D={month}%2F{day}%2F{year}"
        return f"{base_url}?{params}#metro-area-calendar"
    
    def _scrape_page(self, url):
        try:
            page = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            page.raise_for_status()
            soup = BeautifulSoup(page.content, "html.parser")
            
            performers = []
            event_count = 0
            festival_count = 0
            
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    events = data if isinstance(data, list) else [data]
                    
                    for event in events:
                        event_count += 1
                        performer_list = event.get('performer', [])
                        if not isinstance(performer_list, list):
                            performer_list = [performer_list] if performer_list else []
                        
                        if len(performer_list) > Config.FESTIVAL_PERFORMER_LIMIT:
                            festival_count += 1
                            continue
                        
                        event_data = self._extract_event_data(event)
                        for performer in performer_list:
                            name = performer.get('name', '') if isinstance(performer, dict) else str(performer)
                            performers.append({**event_data, 'performer_name': name})
                except Exception:
                    continue
            
            return performers, event_count, festival_count, soup
        except Exception as e:
            print(f"Error scraping page: {e}")
            return [], 0, 0, None
    
    def _extract_event_data(self, event):
        start_date = event.get('startDate', '')
        start_time = ''
        if start_date and 'T' in start_date:
            try:
                dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                start_time = dt.strftime('%H:%M')
            except Exception:
                pass
        
        location = event.get('location', {})
        venue_name = location.get('name', '')
        
        lat = lon = ''
        if location.get('geo'):
            geo = location['geo']
            lat = geo.get('latitude', '')
            lon = geo.get('longitude', '')
        
        distance = Utils.calculate_distance(self.lat, self.lon, lat, lon)
        
        return {
            'venue': venue_name,
            'event_name': event.get('name', ''),
            'event_date': start_date,
            'event_start_time': start_time,
            'latitude': lat,
            'longitude': lon,
            'distance_miles': distance
        }
    
    def _get_next_page_url(self, soup):
        if not soup:
            return None
        try:
            next_link = soup.find("a", class_="next_page")
            return "https://www.songkick.com" + next_link["href"] if next_link else None
        except Exception:
            return None
    
    def _process_performers(self, performers, days_ahead=0):
        if not performers:
            return []
        
        try:
            df = pd.DataFrame(performers)
            df = df[df['venue'] != '']
    
            current_time = datetime.now(self.chicago_tz)
            before_filter_count = len(df)
            df = df[~df.apply(lambda row: self._has_show_started(row, current_time), axis=1)]
            after_filter_count = len(df)
            filtered_out = before_filter_count - after_filter_count
            if filtered_out > 0:
                print(f"Filtered out {filtered_out} performers due to shows already started")

            df['distance_sort'] = pd.to_numeric(df['distance_miles'], errors='coerce')
            df = df.sort_values('distance_sort', na_position='last')
            
            try:
                unique_performers = (df.groupby('performer_name')
                                   .agg({'venue': 'first', 'distance_miles': 'first'})
                                   .reset_index())
            except Exception:
                unique_performers = df.groupby('performer_name').agg({'venue': 'first', 'distance_miles': 'first'}).reset_index()
            
            # Use the target date instead of current time for filename
            target_date = self.get_target_date(days_ahead)
            filename = f'performers_Chicago_{target_date.strftime("%Y-%m-%d_%H-%M")}.csv'
            df.to_csv(filename, index=False)
            print(f"Saved data to: {filename}")
            
            return unique_performers.to_dict('records')
        except Exception as e:
            print(f"Error processing performers: {e}")
            return []

    def _has_show_started(self, row, current_time):
        if not row['event_start_time']:
            return False
        try:
            event_date_str = row['event_date']
            event_time_str = row['event_start_time']
            if 'T' in event_date_str:
                try:
                    dt = datetime.fromisoformat(event_date_str.replace('Z', '+00:00'))
                    event_datetime = self.chicago_tz.localize(dt) if dt.tzinfo is None else dt.astimezone(self.chicago_tz)
                except Exception:
                    date_part = event_date_str.split('T')[0]
                    dt = datetime.strptime(date_part, '%Y-%m-%d')
                    event_time = datetime.strptime(event_time_str, '%H:%M').time()
                    event_datetime = self.chicago_tz.localize(datetime.combine(dt.date(), event_time))
            else:
                event_date = datetime.strptime(event_date_str.split('T')[0], '%Y-%m-%d')
                event_time = datetime.strptime(event_time_str, '%H:%M').time()
                event_datetime = self.chicago_tz.localize(datetime.combine(event_date.date(), event_time))
            return current_time > event_datetime
        except Exception:
            return False

def main():
    print("Starting Automated Concert Discovery and Playlist Creator...")
    
    spotify = SpotifyClient()
    scraper = ConcertScraper(Config.LAT, Config.LON)
    
    if not spotify.authenticate():
        return
    
    performers_data, city, scraped_url = scraper.scrape_concerts(Config.DAYS_IN_FUTURE)
    if not performers_data:
        print("No performers found")
        return
    
    if not spotify.get_search_token():
        print("Could not get Spotify search access")
        return
    
    performer_names = [p['performer_name'] for p in performers_data]
    
    print(f"Searching Spotify for {len(performer_names)} artists...")
    results = []
    for i, artist in enumerate(performer_names, 1):
        if i % 10 == 0 or i == len(performer_names):
            print(f"Processing {i}/{len(performer_names)} artists...")
        result = spotify.search_artist(artist)
        perf_data = next((p for p in performers_data if p['performer_name'] == artist), {})
        result['venue'] = perf_data.get('venue', 'Unknown')
        result['distance_miles'] = perf_data.get('distance_miles', float('inf'))
        results.append(result)
        time.sleep(0.5)
    
    errors = sum(1 for r in results if 'error' in r)
    valid_results = [r for r in results if 'error' not in r]
    print(f"\nFound {len(valid_results)} artists on Spotify ({errors} not found)")
    
    target_date = scraper.get_target_date(Config.DAYS_IN_FUTURE)
    spotify.create_playlist(results, target_date.strftime("%m"), target_date.strftime("%d"), city, scraped_url)

    print("Concert discovery and playlist creation complete!")

if __name__ == "__main__":
    main()