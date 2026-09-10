"""Command-line entry point; no arguments opens the desktop interface."""
import argparse
import json
from dataclasses import asdict
from pathlib import Path
from .scraper import Config, Client, Cancelled, run
from .states import STATES


def main(argv=None):
    parser=argparse.ArgumentParser(description='Bhuvan accepted-geotag downloader')
    sub=parser.add_subparsers(dest='command')
    sub.add_parser('gui',help='Open desktop interface')
    locations=sub.add_parser('locations',help='List live location names and codes')
    locations.add_argument('level',choices=['state','district','block','panchayat'])
    locations.add_argument('--parent',help='Parent location code')
    scrape=sub.add_parser('scrape',help='Download selected accepted geotags')
    scrape.add_argument('--config',type=Path)
    for key in asdict(Config()):
        if key in ('details','resume'): continue
        scrape.add_argument('--'+key.replace('_','-'),type=float if key=='accuracy' else str,default=None)
    scrape.add_argument('--details',action=argparse.BooleanOptionalAction,default=None)
    scrape.add_argument('--resume',action=argparse.BooleanOptionalAction,default=None)
    scrape.add_argument('--dry-run',action='store_true',help='Validate and display configuration without downloading')
    args=parser.parse_args(argv)
    try:
        if args.command in (None,'gui'):
            from .gui import launch
            launch();return 0
        if args.command=='locations':
            if args.level=='state': rows=STATES
            else:
                if not args.parent: parser.error('--parent is required for this level')
                rows=Client().locations(args.level,args.parent)
            print(json.dumps(rows,indent=2));return 0
        values=json.loads(args.config.read_text(encoding='utf-8-sig')) if args.config else {}
        for key in asdict(Config()):
            value=getattr(args,key)
            if value is not None: values[key]=value
        config=Config(**values).validate()
        if args.dry_run:
            print(json.dumps(asdict(config),indent=2));return 0
        return 0 if run(config)['status']=='complete' else 2
    except (Cancelled,KeyboardInterrupt):
        print('Cancelled. Saved panchayats can be resumed.');return 130
    except Exception as exc:
        print(f'Error: {exc}');return 1

if __name__=='__main__':
    raise SystemExit(main())
