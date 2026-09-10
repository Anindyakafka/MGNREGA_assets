"""Shared, resumable accepted-geotag download engine."""
import csv
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from threading import Event

import requests
from .states import STATES

BASE = 'https://bhuvan-app2.nrsc.gov.in/bhugram/bhugram_dashboard/php/'
DETAIL = 'https://bhuvan-app2.nrsc.gov.in/mgnrega/usrtasks/nrega_phase2/get/get_details.php'

class Cancelled(Exception):
    pass

@dataclass
class Config:
    state: str = '05'
    district: str = 'All'
    block: str = 'All'
    panchayat: str = 'All'
    stage: str = '0'
    financial_year: str = 'All'
    category: str = 'All'
    subcategory: str = 'All'
    start_date: str = '2005-07-01'
    end_date: str = field(default_factory=lambda: date.today().isoformat())
    accuracy: float = 0
    details: bool = False
    output: str = 'data/downloads'
    resume: bool = True

    def validate(self):
        for key in ('state','district','block','panchayat','stage','financial_year','category','subcategory'):
            if not isinstance(getattr(self,key), str):
                raise ValueError(f'{key} must be a string; preserve leading zeros')
        if self.state not in STATES:
            raise ValueError('Unknown state code')
        if self.stage not in ('0','1','2','3'):
            raise ValueError('Stage must be 0, 1, 2 or 3')
        for key in ('district','block','panchayat','category','subcategory'):
            if getattr(self,key) != 'All' and not re.fullmatch(r'\d+',getattr(self,key)):
                raise ValueError(f'{key} must be a numeric code or All')
        if self.financial_year != 'All':
            if not re.fullmatch(r'\d{4}-\d{4}',self.financial_year):
                raise ValueError('Financial year must be All or YYYY-YYYY')
            first,last=map(int,self.financial_year.split('-'))
            if last != first+1: raise ValueError('Financial year must span consecutive years')
        for child,parent in [('block','district'),('panchayat','block'),('subcategory','category')]:
            if getattr(self,child) != 'All' and getattr(self,parent) == 'All':
                raise ValueError(f'Select {parent} before {child}')
        if date.fromisoformat(self.start_date) > date.fromisoformat(self.end_date):
            raise ValueError('Start date must not follow end date')
        if not math.isfinite(float(self.accuracy)) or float(self.accuracy)<0:
            raise ValueError('Accuracy must be a finite nonnegative number')
        if not isinstance(self.details,bool) or not isinstance(self.resume,bool):
            raise ValueError('details and resume must be booleans')
        if not self.output:
            raise ValueError('Choose an output directory')
        return self

    def identity(self):
        d=asdict(self)
        for k in ('output','resume'): d.pop(k)
        d['accuracy']=float(d['accuracy'])
        return hashlib.sha256(json.dumps(d,sort_keys=True).encode()).hexdigest()[:20]


def atomic_json(path, value):
    tmp=path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding='utf-8')
    tmp.replace(path)

class Client:
    def __init__(self, cancel=None):
        self.cancel=cancel or Event()

    def check(self):
        if self.cancel.is_set(): raise Cancelled()

    def request(self, endpoint, params=None, html=False):
        for attempt in range(4):
            self.check()
            try:
                if self.cancel.wait(0.25): raise Cancelled()
                if html:
                    r=requests.get(DETAIL,params=params,timeout=(10,30))
                else:
                    r=requests.post(BASE+endpoint,data={'username':'unauthourized',**(params or {})},timeout=(10,30))
                r.raise_for_status()
                if html: return r.text
                rows=r.json()
                if not isinstance(rows,list) or any(not isinstance(x,dict) for x in rows):
                    raise ValueError(f'Unexpected response from {endpoint}')
                return rows
            except (requests.RequestException,ValueError):
                if attempt==3: raise
                if self.cancel.wait(2**attempt): raise Cancelled()

    def locations(self, level, parent, year='All'):
        plural,parent_key={'district':('Districts','state'),'block':('Blocks','district'),'panchayat':('Panchayats','block')}[level]
        rows=self.request(f'location/get{plural}.php',{parent_key+'_code':parent,'financial_year':year})
        if any(level+'_code' not in r or level+'_name' not in r for r in rows):
            raise ValueError(f'Invalid {level} lookup response')
        return [r for r in rows if str(r[level+'_code'])!='All']


def selected(rows, level, code):
    result=[r for r in rows if code=='All' or str(r[level+'_code'])==code]
    if not result: raise ValueError(f'No {level} found for selection {code}')
    return result


def parse_details(html):
    from bs4 import BeautifulSoup
    soup=BeautifulSoup(html,'html.parser')
    mapping={'Category':'Category','Sub-Category':'Sub-Category','Asset Name':'Asset Name','Work Name':'Work Name','Work Type':'Work Type','Cumulative Cost of Asset':'Estimated Cost','Expenditure Unskilled':'Unskilled','Expenditure Material/Skilled':'Material/Skilled','Work Start Date':'Work_start_date'}
    result={v:None for v in mapping.values()}
    cells=soup.find_all('td')
    for a,b in zip(cells,cells[1:]):
        key=a.get_text(' ',strip=True).rstrip(':').strip()
        if key in mapping: result[mapping[key]]=b.get_text(' ',strip=True) or None
    if not any(v is not None for v in result.values()):
        raise ValueError('Detail page has no recognized asset fields')
    return result


def run(config, emit=print, cancel=None, client=None):
    config.validate()
    client=client or Client(cancel)
    root=Path(config.output).expanduser().resolve()/f'{config.state}_{config.identity()}'
    root.mkdir(parents=True,exist_ok=True)
    lock=root/'.running'
    try: lock_fd=lock.open('x')
    except FileExistsError: raise RuntimeError(f'Run directory is locked: {lock}. If a previous process crashed, remove this file after verifying it has stopped.')
    try:
        lock_fd.close()
        return _run(config,root,emit,client)
    finally: lock.unlink(missing_ok=True)


def _run(c,root,emit,client):
    atomic_json(root/'config.json',asdict(c))
    parts=root/'panchayats'; parts.mkdir(exist_ok=True)
    errors=[]; files=[]; done=0
    def log(message):
        emit(message)
        with (root/'run.log').open('a',encoding='utf-8') as f: f.write(message+'\n')
    atomic_json(root/'status.json',{'status':'running'})
    try:
        districts=selected(client.locations('district',c.state,c.financial_year),'district',c.district)
        for d in districts:
            dc=str(d['district_code'])
            blocks=selected(client.locations('block',dc,c.financial_year),'block',c.block)
            for b in blocks:
                bc=str(b['block_code'])
                ps=selected(client.locations('panchayat',bc,c.financial_year),'panchayat',c.panchayat)
                for p in ps:
                    client.check()
                    pc=str(p['panchayat_code'])
                    name=hashlib.sha256(f'{dc}/{bc}/{pc}'.encode()).hexdigest()[:24]
                    part=parts/(name+'.json')
                    try:
                        cached=None
                        if c.resume and part.exists():
                            try:
                                cached=json.loads(part.read_text(encoding='utf-8'))
                            except (ValueError,OSError):
                                log(f'Unreadable checkpoint for {pc}; downloading again')
                        if isinstance(cached,dict) and cached.get('complete') is True and isinstance(cached.get('rows'),list):
                            files.append(part); done+=1
                            log(f'Resumed {p["panchayat_name"]} ({done} panchayats)'); continue
                        params={'stage':c.stage,'state_code':c.state,'district_code':dc,'block_code':bc,'panchayat_code':pc,'financial_year':c.financial_year,'accuracy':c.accuracy,'category_id':c.category,'sub_category_id':c.subcategory,'start_date':c.start_date,'end_date':c.end_date}
                        rows=client.request('reports/accepted_geotags.php',params)
                        complete=True
                        for row in rows:
                            if not {'collection_sno','lat','lon'}.issubset(row):
                                raise ValueError('Geotag response is missing identifiers or coordinates')
                            row.update(State=STATES[c.state],District=d['district_name'],Block=b['block_name'],Panchayat=p['panchayat_name'],State_ID=c.state,District_ID=dc,Block_ID=bc,Panchayat_ID=pc,Stage=c.stage)
                            if c.details:
                                client.check()
                                try:
                                    row.update(parse_details(client.request('',{'sno':row['collection_sno']},html=True)))
                                    row['detail_status']='complete'
                                except Cancelled: raise
                                except Exception as exc:
                                    complete=False; row['detail_status']='failed'; row['detail_error']=str(exc)
                        atomic_json(part,{'complete':complete,'rows':rows})
                        files.append(part);done+=1
                        if not complete: errors.append({'panchayat':pc,'error':'Some detail downloads failed; resume to retry'})
                        log(f'{d["district_name"]} / {b["block_name"]} / {p["panchayat_name"]}: {len(rows)} rows ({done} panchayats)')
                    except Cancelled: raise
                    except Exception as exc:
                        errors.append({'panchayat':pc,'error':str(exc)});log(f'FAILED {pc}: {exc}')
        # Stream partition files twice; avoid loading a whole state into memory.
        fields=[]
        for part in files:
            client.check()
            for row in json.loads(part.read_text(encoding='utf-8'))['rows']:
                for key in row:
                    if key not in fields: fields.append(key)
        fields=fields or ['collection_sno','lat','lon','State_ID','District_ID','Block_ID','Panchayat_ID','Stage']
        tmp=root/'assets.csv.tmp'; count=0
        with tmp.open('w',newline='',encoding='utf-8-sig') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader()
            for part in files:
                client.check()
                for row in json.loads(part.read_text(encoding='utf-8'))['rows']:
                    writer.writerow(row);count+=1
        tmp.replace(root/'assets.csv')
        status={'status':'partial' if errors else 'complete','rows':count,'panchayats':done,'errors':errors,'output':str(root/'assets.csv')}
        atomic_json(root/'status.json',status)
        log(f'{status["status"].upper()}: {count} rows. {root / "assets.csv"}')
        return status
    except BaseException as exc:
        atomic_json(root/'status.json',{'status':'cancelled' if isinstance(exc,(Cancelled,KeyboardInterrupt)) else 'failed','error':str(exc),'errors':errors})
        raise
