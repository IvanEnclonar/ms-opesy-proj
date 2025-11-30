import requests
from bs4 import BeautifulSoup
import multiprocessing as mp
from multiprocessing import Process
from multiprocessing.managers import BaseManager
import queue
import time
import argparse
import csv
from urllib.parse import urljoin, urlparse
import urllib3
import sys
import os

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

job_queue = queue.Queue()
visited_urls = {}
found_urls = {}
stop_event = mp.Event()

def get_job_queue(): return job_queue
def get_visited(): return visited_urls
def get_found(): return found_urls
def get_stop_event(): return stop_event

class DistributedManager(BaseManager):
    pass

DistributedManager.register('get_job_queue', callable=get_job_queue, proxytype=None)  # queue proxy
DistributedManager.register('get_visited', callable=get_visited, proxytype=None) # dict proxy
DistributedManager.register('get_found', callable=get_found, proxytype=None)     # dict proxy
DistributedManager.register('get_stop_event', callable=get_stop_event)

def scrape_worker(base_url, urls_to_visit, visited_urls, found_urls_dict, stop_event):
    print("Worker online")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

    while not stop_event.is_set():
        try:
            current_url = urls_to_visit.get(timeout=1)
        except queue.Empty:
            continue

        if visited_urls.get(current_url) is True:
            continue
        
        visited_urls.update({current_url: True})

        try:
            print(f"Fetching: {current_url}")
            response = requests.get(current_url, timeout=10, headers=headers, verify=False)
            
            if response.status_code != 200:
                print(f"Error {response.status_code} accessing {current_url}")
                continue
                
        except requests.RequestException as e:
            print(f"Network Error on {current_url}: {e}")
            continue

        try:
            soup = BeautifulSoup(response.text, 'html.parser')
            base_netloc = urlparse(base_url).netloc
            links_found = 0

            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href.startswith(('mailto:', 'tel:', 'javascript:')): continue
                    
                next_url = urljoin(current_url, href).split('#')[0].rstrip('/')
                parsed_next = urlparse(next_url)
                
                if parsed_next.netloc.endswith(base_netloc.replace('www.', '')) and parsed_next.scheme in ['http', 'https']:
                    
                    # Check server found dict
                    if found_urls_dict.get(next_url) is None:
                        description = link.get_text(strip=True).strip() or "N/A"
                        found_urls_dict.update({next_url: description})
                        
                    # Add to queue if not seen before
                    if visited_urls.get(next_url) is None:
                        if not any(next_url.lower().endswith(ext) for ext in ['.pdf', '.jpg', '.png', '.zip']):
                            urls_to_visit.put(next_url)
                            links_found += 1
            
            print(f"Parsed {current_url} - Added {links_found} new links.")

        except Exception as e:
            print(f"Parsing Error on {current_url}: {e}")

# Checkpoint Logic for the server
def run_server_monitor(manager, base_url, duration, start_time):
    urls_to_visit = manager.get_job_queue()
    visited_proxy = manager.get_visited()
    found_proxy = manager.get_found()
    stop_signal = manager.get_stop_event()

    checkpoints = [5, 15, 30, 60, 300, 600]
    saved_checkpoints = set()
    
    print(f"Program Duration: {duration}s")

    try:
        while True:
            elapsed = int(time.time() - start_time)
            
            # Snapshot stats
            try:
                q_size = urls_to_visit.qsize()
                v_count = len(visited_proxy)
                u_count = len(found_proxy)
                print(f"Queue: {q_size} | Visited: {v_count} | URLs Found: {u_count}   ", end="\r")
            except:
                pass

            # Checkpoint Saving
            for cp in checkpoints:
                if elapsed >= cp and cp not in saved_checkpoints and cp <= duration:
                    print(f"\nSaving Checkpoint at {cp}s")
                    
                    found_copy = dict(found_proxy.items()) 
                    visited_copy = dict(visited_proxy.items())
                    pages_scraped_count = sum(1 for v in visited_copy.values() if v is True)
                    total_found_count = len(found_copy)

                    output_dir = "output"
                    if not os.path.exists(output_dir):
                        os.makedirs(output_dir)
                    
                    # csv file
                    csv_filename = f"{output_dir}/distributed_checkpoint_{cp}s.csv"
                    with open(csv_filename, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['url', 'description'])
                        for k, v in found_copy.items():
                            writer.writerow([k, v])
                    
                    # text file
                    txt_filename = f"{output_dir}/summary_checkpoint_{cp}s.txt"
                    with open(txt_filename, 'w', encoding='utf-8') as f:
                        f.write("Scraper Summary\n")
                        f.write("==============================\n")
                        f.write(f"Base URL: {base_url}\n")
                        f.write(f"Checkpoint Time: {elapsed} second(s)\n")
                        f.write(f"Total uration: {duration} second(s)\n\n")
                        
                        f.write(f"Total Pages Scraped (Unique URLs Accessed): {pages_scraped_count}\n")
                        f.write(f"Total Unique URLs Found : {total_found_count}\n\n")
                        
                        f.write("==============================\n")
                        f.write("List of All URLs Found:\n")

                        for url in found_copy.keys():
                            f.write(f"- {url}\n")

                    print(f"Saved CSV and Summary to {output_dir}/")
                    saved_checkpoints.add(cp)

            if elapsed >= duration:
                print("\nTime limit reached, stopping cluster")
                stop_signal.set()
                break
            
            if elapsed > 20 and q_size == 0 and urls_to_visit.empty():
                print("\nQueue empty, stopping cluster")
                stop_signal.set()
                break

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nServer interrupted manually")
        stop_signal.set()

def main():
    parser = argparse.ArgumentParser(description="Distributed Web Scraper")
    parser.add_argument("--mode", choices=['server', 'client'], required=True, help="Run as server or client (worker)")
    parser.add_argument("--ip", default="127.0.0.1", help="IP address of the server (default: localhost)")
    parser.add_argument("--port", type=int, default=50000, help="Port number (default: 50000)")
    parser.add_argument("--auth", default="secret", help="Auth key")    
    parser.add_argument("--url", help="Start URL (e.g., https://www.dlsu.edu.ph). Required for Server AND Client.")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds (Server only)")
    parser.add_argument("--procs", type=int, default=mp.cpu_count(), help="Number of worker processes (Client only)")

    args = parser.parse_args()
    auth_key = args.auth.encode('utf-8')

    if args.url:
        args.url = args.url.rstrip('/')

    if args.mode == 'server':
        if not args.url:
            print("Error: Server requires --url")
            return

        manager = DistributedManager(address=('', args.port), authkey=auth_key)
        manager.start()
        
        q = manager.get_job_queue()
        q.put(args.url)
        
        # Initialize visited dict with base URL to avoid re-adding it
        visited = manager.get_visited()
        visited.update({args.url: False}) 

        print(f"Server started on Port {args.port}.")
        
        run_server_monitor(manager, args.url, args.duration, time.time())
        
        print("Waiting 5s for clients to detach")
        time.sleep(5)
        manager.shutdown()

    elif args.mode == 'client':
        if not args.url:
            print("Error: Client requires --url to filter.")
            return

        print(f"Connecting to Server at {args.ip}:{args.port}")
        
        manager = DistributedManager(address=(args.ip, args.port), authkey=auth_key)
        try:
            manager.connect()
            print("Connected to Server!")
        except ConnectionRefusedError:
            print("Connection Failed")
            return

        # Shared Objects
        urls_to_visit = manager.get_job_queue()
        visited_urls = manager.get_visited()
        found_urls_dict = manager.get_found()
        stop_event = manager.get_stop_event()

        print(f"Spawning {args.procs} worker processes")
        processes = []
        for _ in range(args.procs):
            p = Process(target=scrape_worker, args=(
                args.url, 
                urls_to_visit, 
                visited_urls, 
                found_urls_dict, 
                stop_event
            ))
            p.start()
            processes.append(p)

        # Main thread waits for the stop signal
        while not stop_event.is_set():
            time.sleep(1)
        
        print("Shutting down workers")
        for p in processes:
            p.join()
        print("Client Shutdown")

if __name__ == "__main__":
    mp.freeze_support()
    main()