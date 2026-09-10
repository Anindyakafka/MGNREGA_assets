"""Tk desktop client. Network operations never access Tk widgets."""
import json
import queue
import threading
from dataclasses import asdict
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from .scraper import Config, Client, Cancelled, run
from .states import STATES

STAGES={'0':'Phase I: Assets','1':'Phase II: Before','2':'Phase II: During','3':'Phase II: After'}

class App:
    def __init__(self,root):
        self.root=root;root.title('MGNREGA / Bhuvan asset downloader');root.geometry('1000x820')
        self.events=queue.Queue();self.cancel=threading.Event();self.running=False;self.lookups=0
        self.vars={};self.boxes={};self.maps={};self.versions={};self.jobs=[]
        pane=ttk.Frame(root,padding=15);pane.pack(fill='both',expand=True)
        ttk.Label(pane,text='Download accepted geotags',font=('',16,'bold')).pack(anchor='w')
        ttk.Label(pane,text='Choose an area and filters. Each exported row is a geotag; repeated works are preserved.').pack(anchor='w',pady=(3,12))
        form=ttk.Frame(pane);form.pack(fill='x');form.columnconfigure(1,weight=1);form.columnconfigure(3,weight=1)
        labels={'state':'State / UT','district':'District','block':'Block','panchayat':'Panchayat','stage':'Stage','financial_year':'Financial year','category':'Asset category','subcategory':'Asset subcategory','start_date':'From date (YYYY-MM-DD)','end_date':'To date (YYYY-MM-DD)','accuracy':'Accuracy greater than (m)','output':'Output folder'}
        defaults=asdict(Config())
        for i,(key,label) in enumerate(labels.items()):
            row,col=divmod(i,2);col*=2
            ttk.Label(form,text=label).grid(row=row,column=col,sticky='w',padx=5,pady=5)
            v=tk.StringVar(value=str(defaults[key]));self.vars[key]=v
            if key in ('state','district','block','panchayat','stage','financial_year','category','subcategory'):
                widget=ttk.Combobox(form,textvariable=v,state='readonly',width=29)
                self.boxes[key]=widget;self.set_options(key,{'All':'All'})
                widget.bind('<<ComboboxSelected>>',lambda e,k=key:self.changed(k))
            else: widget=ttk.Entry(form,textvariable=v)
            widget.grid(row=row,column=col+1,sticky='ew',padx=5,pady=5)
        self.set_options('state',STATES,'05');self.set_options('stage',STAGES,'0')
        checks=ttk.Frame(pane);checks.pack(fill='x',pady=8)
        for key,label in [('details','Download work details (slower)'),('resume','Resume saved panchayats')]:
            self.vars[key]=tk.BooleanVar(value=defaults[key]);ttk.Checkbutton(checks,text=label,variable=self.vars[key]).pack(side='left',padx=5)
        ttk.Button(checks,text='Browse output',command=self.browse).pack(side='right')
        buttons=ttk.Frame(pane);buttons.pack(fill='x',pady=5)
        for label,fn in [('Save settings',self.save),('Load settings',self.load),('Copy command',self.copy),('Refresh lists',self.refresh),('Add selection to queue',self.add)]:
            ttk.Button(buttons,text=label,command=fn).pack(side='left',padx=3)
        self.joblist=tk.Listbox(pane,height=4);self.joblist.pack(fill='x',pady=5)
        ttk.Button(pane,text='Remove selected queued area',command=self.remove).pack(anchor='w')
        controls=ttk.Frame(pane);controls.pack(fill='x',pady=8)
        self.start=ttk.Button(controls,text='Start download',command=self.start_run);self.start.pack(side='left')
        self.stop=ttk.Button(controls,text='Cancel',command=self.cancel.set,state='disabled');self.stop.pack(side='left',padx=8)
        self.status=tk.StringVar(value='Loading available locations and filters...')
        ttk.Label(controls,textvariable=self.status).pack(side='left')
        self.progress=ttk.Progressbar(pane,mode='indeterminate');self.progress.pack(fill='x')
        self.log=tk.Text(pane,height=12,wrap='word',state='disabled');self.log.pack(fill='both',expand=True,pady=8)
        root.protocol('WM_DELETE_WINDOW',self.close);root.after(100,self.poll);self.refresh()

    def set_options(self,key,options,selected='All'):
        self.maps[key]={f'{name} [{code}]':str(code) for code,name in options.items()}
        values=list(self.maps[key]);self.boxes[key]['values']=values
        label=next((label for label,code in self.maps[key].items() if code==selected),values[0] if values else '')
        self.vars[key].set(label)

    def code(self,key): return self.maps[key].get(self.vars[key].get(),self.vars[key].get())

    def lookup(self,key,endpoint,params,code,name):
        self.versions[key]=self.versions.get(key,0)+1;version=self.versions[key];self.lookups+=1
        self.boxes[key].configure(state='disabled')
        def worker():
            try: self.events.put(('options',key,version,{str(r[code]):r[name] for r in Client().request(endpoint,params)}))
            except Exception as exc: self.events.put(('lookup_error',key,version,str(exc)))
        threading.Thread(target=worker,daemon=True).start()

    def changed(self,key):
        chain=['state','district','block','panchayat']
        if key in chain:
            index=chain.index(key)
            for child in chain[index+1:]:
                self.versions[child]=self.versions.get(child,0)+1
                self.set_options(child,{'All':'All'});self.boxes[child].configure(state='readonly')
            if index<3 and self.code(key)!='All':
                child=chain[index+1]
                self.lookup(child,f'location/get{child.capitalize()}s.php',{key+'_code':self.code(key),'financial_year':self.code('financial_year')},child+'_code',child+'_name')
        if key=='category':
            self.versions['subcategory']=self.versions.get('subcategory',0)+1
            self.set_options('subcategory',{'All':'All'})
            if self.code(key)!='All': self.lookup('subcategory','reports/getSubCategories.php',{'category_id':self.code(key)},'sub_category_id','sub_category')
        if key=='financial_year': self.changed('state')

    def refresh(self):
        self.changed('state')
        self.versions['subcategory']=self.versions.get('subcategory',0)+1
        self.set_options('subcategory',{'All':'All'})
        self.boxes['subcategory'].configure(state='readonly')
        self.lookup('category','reports/getCategories.php',{},'category_id','category_name')
        self.lookup('financial_year','getFinancialYears.php',{'report':'4'},'financial_year','financial_year')

    def config(self):
        if self.lookups: raise ValueError('Wait for location/filter lists to finish loading')
        values={key:(self.code(key) if key in self.maps else v.get()) for key,v in self.vars.items()}
        values['accuracy']=float(values['accuracy'])
        return Config(**values).validate()

    def browse(self):
        path=filedialog.askdirectory()
        if path:self.vars['output'].set(path)

    def save(self):
        try:
            c=self.config();path=filedialog.asksaveasfilename(defaultextension='.json',filetypes=[('Settings','*.json')])
            if path: Path(path).write_text(json.dumps(asdict(c),indent=2),encoding='utf-8')
            return path
        except Exception as exc:messagebox.showerror('Settings',str(exc))

    def load(self):
        path=filedialog.askopenfilename(filetypes=[('Settings','*.json')])
        if not path:return
        try:
            c=Config(**json.loads(Path(path).read_text(encoding='utf-8-sig'))).validate()
            # Keep exact saved codes; the engine validates parent membership live.
            for key,value in asdict(c).items():
                if key in self.maps:
                    self.versions[key]=self.versions.get(key,0)+1
                    options={code:label.rsplit(' [',1)[0] for label,code in self.maps[key].items()}
                    options.setdefault(value,value);self.set_options(key,options,value)
                    self.boxes[key].configure(state='readonly')
                else:self.vars[key].set(value)
            self.status.set('Settings loaded. Refresh lists to choose a different area.')
        except Exception as exc:messagebox.showerror('Settings',str(exc))

    def copy(self):
        path=self.save()
        if path:
            command="python app.py scrape --config '"+str(Path(path).resolve()).replace("'", "''")+"'"
            self.root.clipboard_clear();self.root.clipboard_append(command)
            self.status.set('Command copied (run from the project folder)')

    def add(self):
        if self.running:
            messagebox.showinfo('Queue','Wait for the current queue to finish before adding selections.')
            return
        try:
            c=self.config();self.jobs.append(c)
            self.joblist.insert('end',f'{STATES[c.state]} / {c.district} / {c.block} / {c.panchayat} | stage {c.stage} | {c.start_date} to {c.end_date}')
        except Exception as exc:messagebox.showerror('Selection',str(exc))

    def remove(self):
        if self.running:return
        for index in reversed(self.joblist.curselection()):del self.jobs[index];self.joblist.delete(index)

    def start_run(self):
        if self.running:return
        try: jobs=list(self.jobs) or [self.config()]
        except Exception as exc:messagebox.showerror('Selection',str(exc));return
        self.running=True;self.cancel.clear();self.start.configure(state='disabled');self.stop.configure(state='normal');self.progress.start()
        self.status.set('Downloading; see progress below')
        def worker():
            failures=0
            try:
                for c in jobs:
                    if self.cancel.is_set():raise Cancelled()
                    result=run(c,lambda msg:self.events.put(('log',msg)),self.cancel)
                    failures+=result['status']!='complete'
                self.events.put(('done','Finished with incomplete downloads; see log' if failures else 'Download complete'))
            except Cancelled:self.events.put(('done','Cancelled; saved panchayats can be resumed'))
            except Exception as exc:self.events.put(('done','Failed: '+str(exc)))
        threading.Thread(target=worker,daemon=True).start()

    def poll(self):
        try:
            while True:
                event=self.events.get_nowait();kind=event[0]
                if kind in ('options','lookup_error'):
                    _,key,version,payload=event;self.lookups-=1
                    if self.versions.get(key)!=version:continue
                    self.boxes[key].configure(state='readonly')
                    if kind=='options':self.set_options(key,{'All':'All',**payload},self.code(key))
                    else:self.write('Lookup failed: '+payload)
                    if not self.running:self.status.set('Ready' if not self.lookups else 'Loading lists...')
                elif kind=='log':self.write(event[1])
                elif kind=='done':
                    self.running=False;self.progress.stop();self.start.configure(state='normal');self.stop.configure(state='disabled');self.status.set(event[1]);self.write(event[1])
        except queue.Empty:pass
        self.root.after(100,self.poll)

    def write(self,message):
        self.log.configure(state='normal');self.log.insert('end',message+'\n');self.log.see('end');self.log.configure(state='disabled')

    def close(self):
        if self.running:
            self.cancel.set();self.status.set('Cancelling; close again after the download stops');return
        self.root.destroy()

def launch():
    root=tk.Tk();App(root);root.mainloop()
