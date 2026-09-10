import sys,unittest,tempfile,json,csv
from pathlib import Path
from threading import Event
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from mgnrega_assets.scraper import Config,Client,Cancelled,run,parse_details

class Fake(Client):
    def __init__(self):super().__init__();self.calls=0;self.fail=False;self.detail_fail=False
    def locations(self,level,parent,year='All'):
        code={'district':'0541','block':'0541001','panchayat':'0541001001'}[level]
        return [{level+'_code':code,level+'_name':level}]
    def request(self,endpoint,params=None,html=False):
        if html:
            if self.detail_fail:raise ValueError('temporary detail failure')
            return '<table><tr><td>Asset Name</td><td>Pond</td></tr></table>'
        self.calls+=1
        if self.fail:raise ValueError('bad response')
        return [{'collection_sno':str(i),'workcode':'same-work','lat':'25','lon':'85'} for i in (1,2)]

class Tests(unittest.TestCase):
    def test_validation(self):
        for args in [{'state':5},{'block':'1'},{'start_date':'2027-01-01','end_date':'2026-01-01'},{'accuracy':float('nan')},{'details':'false'},{'stage':'4'}]:
            with self.assertRaises((ValueError,TypeError)):Config(**args).validate()
    def test_identity(self):
        self.assertEqual(Config().identity(),Config(output='elsewhere',resume=False,accuracy=0.0).identity())
        self.assertNotEqual(Config().identity(),Config(stage='1').identity())
    def test_preserves_and_resumes(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=Config(output=tmp);client=Fake()
            first=run(c,lambda x:None,client=client)
            self.assertEqual(first['rows'],2)
            second=run(c,lambda x:None,client=client)
            self.assertEqual(client.calls,1);self.assertEqual(second['rows'],2)
            c.resume=False;run(c,lambda x:None,client=client);self.assertEqual(client.calls,2)
    def test_failed_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=Config(output=tmp);client=Fake();client.fail=True
            self.assertEqual(run(c,lambda x:None,client=client)['status'],'partial')
            client.fail=False
            self.assertEqual(run(c,lambda x:None,client=client)['status'],'complete')
    def test_detail_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=Config(output=tmp,details=True);client=Fake();client.detail_fail=True
            self.assertEqual(run(c,lambda x:None,client=client)['status'],'partial')
            client.detail_fail=False
            result=run(c,lambda x:None,client=client)
            self.assertEqual(result['status'],'complete');self.assertEqual(client.calls,2)
            with open(result['output'],encoding='utf-8-sig') as f:
                row=next(csv.DictReader(f));self.assertEqual(row['Asset Name'],'Pond')
    def test_cancel(self):
        with tempfile.TemporaryDirectory() as tmp:
            client=Fake();client.cancel.set()
            with self.assertRaises(Cancelled):run(Config(output=tmp),lambda x:None,client=client)
            self.assertFalse(list(Path(tmp).rglob('.running')))
            status=json.loads(next(Path(tmp).rglob('status.json')).read_text())
            self.assertEqual(status['status'],'cancelled')
    def test_wrong_parent(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):run(Config(output=tmp,district='9999'),lambda x:None,client=Fake())
    def test_parser(self):
        with self.assertRaises(ValueError):parse_details('<html>Service unavailable</html>')
        row=parse_details('<td>Asset Name</td><td>Pond</td>')
        self.assertIsNone(row['Estimated Cost']);self.assertNotIn('Total_persondays',row)

    def test_empty_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            from unittest.mock import Mock
            client=Fake();client.request=Mock(return_value=[])
            c=Config(output=tmp)
            self.assertEqual(run(c,lambda x:None,client=client)['rows'],0)
            run(c,lambda x:None,client=client)
            self.assertEqual(client.request.call_count,1)

    def test_corrupt_checkpoint(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=Config(output=tmp);client=Fake()
            run(c,lambda x:None,client=client)
            next(Path(tmp).rglob('panchayats/*.json')).write_text('invalid')
            self.assertEqual(run(c,lambda x:None,client=client)['status'],'complete')
            self.assertEqual(client.calls,2)

    def test_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            c=Config(output=tmp);root=Path(tmp)/f'{c.state}_{c.identity()}'
            root.mkdir();(root/'.running').touch()
            with self.assertRaises(RuntimeError):run(c,lambda x:None,client=Fake())
            self.assertTrue((root/'.running').exists())

    def test_cli_overrides(self):
        import contextlib,io
        from mgnrega_assets.cli import main
        with tempfile.TemporaryDirectory() as tmp:
            config=Path(tmp)/'settings.json';config.write_text(json.dumps({'state':'05','stage':'0','details':True}))
            out=io.StringIO()
            with contextlib.redirect_stdout(out):
                code=main(['scrape','--config',str(config),'--stage','2','--no-details','--dry-run'])
            self.assertEqual(code,0)
            data=json.loads(out.getvalue());self.assertEqual(data['stage'],'2');self.assertFalse(data['details'])

if __name__=='__main__':unittest.main()
