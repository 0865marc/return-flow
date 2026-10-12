"""Archived experiment: one 2,000 requests/s comparison with three app CPUs.

Direct API: 3 CPUs. Events: API 2 CPUs and worker 1 CPU. Each variant sends
for 15 seconds, waits for HTTP responses, then allows 15 seconds of passive drain.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from uuid import uuid4

REPO=Path(__file__).resolve().parents[3]
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(REPO/'benchmarks'))
import run
from analytics import _statistics

def run_case(mode, root, settings, *, resource_counters=False, build=False):
    queued=mode=='event_driven'
    output=root/mode; output.mkdir()
    env=run.environment(settings,mode,output)
    env.update(BENCH_API_CPUS='2' if queued else '3',BENCH_API_WORKERS='2' if queued else '3',
        BENCH_WORKER_CPUS='1')
    project='rfbench-'+uuid4().hex[:16]
    compose=run.Compose(project,env)
    resolved=json.loads(compose('config','--format','json',capture=True).stdout)
    # Keep the production publisher default explicit: 64 per API process.
    resolved['services']['api']['environment']['RABBITMQ_PUBLISH_CONCURRENCY']='64'
    compose.config_path=output/'compose.resolved.json';run.save(compose.config_path,resolved)
    plan=run.make_plan(str(uuid4()),'return',30000)
    run.save(output/'plan.json',plan)
    config={'mode':mode,'operation':'return','rate':2000,'duration':15,'timeout':10,
        'repetitions':1,'passive_drain_seconds':15,'project':project,'generator':'k6:1.6.1',
        'generator_vus':512,'instrumentation':False,'resource_counters':resource_counters,
        'resources':{k:v for k,v in env.items() if k.startswith('BENCH_') and k.endswith(('CPUS','WORKERS','SIZE','CONCURRENCY'))},
        'publisher_concurrency_per_process':64 if queued else None}
    run.save(output/'config.json',config)
    services=['api','worker'] if queued else ['api']
    infrastructure=['postgres','rabbitmq'] if queued else ['postgres']
    groups={}
    def counters():
        return {'monotonic':time.monotonic(),'services':{service:{name:(group/name).read_text()
            for name in ('cpu.stat','cpu.max','io.stat','memory.current')} for service,group in groups.items()}}
    try:
        if build:
            compose('build','api')
        compose('up','-d','--no-deps',*infrastructure)
        compose.driver('prepare')
        compose('up','-d','--no-deps',*services)
        compose.driver('ready')
        limits={}
        for service in services+infrastructure:
            cid=compose('ps','-q',service,capture=True).stdout.strip()
            item=json.loads(subprocess.check_output(['docker','inspect',cid]))[0]
            if resource_counters:
                group=Path(f"/proc/{item['State']['Pid']}/cgroup").read_text().strip().split('::',1)[1]
                groups[service]=Path('/sys/fs/cgroup'+group)
            limits[service]={'image':item['Image'],'NanoCpus':item['HostConfig']['NanoCpus'],'command':item['Config']['Cmd']}
        assert limits['api']['NanoCpus']==(2 if queued else 3)*1_000_000_000
        if queued: assert limits['worker']['NanoCpus']==1_000_000_000
        run.save(output/'actual-limits.json',limits)
        print(f'LOAD {mode}: 2000/s, 15 s, API {2 if queued else 3} CPU/processes'+(', worker 1 CPU' if queued else ''),flush=True)
        before=counters()
        k6=subprocess.run(['docker','run','--rm','--cpus','4','--network',project+'_default',
            '--user',f'{os.getuid()}:{os.getgid()}',
            '-v',str(output)+':/results','-v',str(HERE)+':/benchmark:ro',
            'grafana/k6:1.6.1','run','--quiet','--no-usage-report','--log-output=none','/benchmark/load.js'],check=False)
        after=counters()
        time.sleep(15)
        compose('stop','--timeout','10',*services)
        if compose('ps','--status','running','-q',*services,capture=True).stdout.strip():
            raise RuntimeError('Application still running; refusing measurement')
        run.save(output/'counters.json',{'enabled':resource_counters,'before':before,'after_http':after,
            'note':'Includes k6 initialization and HTTP drain, excludes passive command drain.'})
        print('MEASURE '+mode,flush=True)
        compose.driver('snapshot')
        summary=json.loads((output/'k6-summary.json').read_text())
        diagnostic=summary['admission_diagnostic']
        counts=diagnostic['counts']
        snapshot=json.loads((output/'snapshot.json').read_text())
        attempted=counts['requests_attempted']
        expected_ids={r['command_id'] for r in plan[:attempted]}
        observed_ids={r['command_id'] for r in snapshot}
        counter_window=after['monotonic']-before['monotonic']
        cpu={}
        for name,a in after['services'].items():
            last=dict((k,int(v)) for k,v in (r.split() for r in a['cpu.stat'].splitlines()))
            first=dict((k,int(v)) for k,v in (r.split() for r in before['services'][name]['cpu.stat'].splitlines()))
            cpu[name]={'used_seconds':(last['usage_usec']-first['usage_usec'])/1e6,
                'average_cores':(last['usage_usec']-first['usage_usec'])/1e6/counter_window,
                'throttled_seconds':(last['throttled_usec']-first['throttled_usec'])/1e6}
        completed=[r for r in snapshot if r['status']=='completed']
        internal=[(datetime.fromisoformat(r['completed_at'])-datetime.fromisoformat(r['created_at'])).total_seconds()*1000 for r in completed]
        result={'directory':str(output),'mode':mode,'k6_exit_code':k6.returncode,'counts':counts,
            'accepted_per_submission_second':counts['accepted']/15,
            'http_ms':summary['metrics']['http_req_duration']['values'],
            'wall_ms':summary['metrics']['diag_request_wall_ms']['values'],
            'completed':len(completed),'valid_completed':sum(r['business_valid'] for r in completed),
            'failed':sum(r['status']=='failed' for r in snapshot),'pending':sum(r['status']=='pending' for r in snapshot),
            'missing_attempted':len(expected_ids-observed_ids),'unexpected_commands':len(observed_ids-expected_ids),
            'database_execution_ms':_statistics(internal),'cpu':cpu,'cpu_counter_interval_s':counter_window,
            'client_timing':diagnostic.get('timing',{}),
            'notes':['Accepted rate divides all successful responses by 15 s of submission; includes responses finishing during HTTP drain.',
                'Database execution starts at command insertion and excludes prior queueing, and completed_at precedes COMMIT.',
                'A single repetition per variant; metrics do not estimate a confidence interval.']}
        run.save(output/'report.json',result)
        print(json.dumps(result,indent=2),flush=True)
        return result
    finally:
        compose('down','--volumes','--remove-orphans','--timeout','10',check=False)

def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-build',action='store_true',
        help='Reuse return-flow-benchmark-v2:local instead of building the current API.')
    parser.add_argument('--resource-counters',action='store_true',
        help='Read cgroup v2 counters before and after HTTP; requires local Linux Docker and readable host cgroups.')
    args=parser.parse_args(argv)
    root=REPO/'benchmarks/results'/('three-cpu-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid4().hex[:8])
    root.mkdir(parents=True)
    print('RESULTS_ROOT='+str(root),flush=True)
    settings=run.arguments(['--rates','2000','--duration','15','--repetitions','1',
        '--drain-seconds','15','--concurrency','512','--app-cpus','3','--db-cpus','1',
        '--broker-cpus','1','--client-cpus','4','--pool-size','10','--worker-concurrency','8'])
    # Archive the exact local scripts even if a later infrastructure step fails.
    for name in ('run_pair.py','load.js','completion.py'):
        shutil.copyfile(HERE/name,root/name)
    comparison={'methodology':'Two sequential isolated runs, identical generator and infrastructure quotas, application CPU redistribution requested by user',
        'rate':2000,'duration_seconds':15,'repetitions_per_variant':1,'results':[]}
    for index,mode in enumerate(('postgres_pool','event_driven')):
        comparison['results'].append(run_case(mode,root,settings,
            resource_counters=args.resource_counters,build=index==0 and not args.no_build))
        run.save(root/'comparison.json',comparison)
    print('COMPARISON='+str(root/'comparison.json'),flush=True)


if __name__=='__main__':
    main()
