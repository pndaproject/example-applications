#!/usr/bin/env python
import time
import subprocess
out = subprocess.check_output(['yarn','application','-list'])

is_running = False

for line in out.splitlines():
    fields = line.split('\t')
    if len(fields) >= 6:
        app = fields[1].strip()
        state = fields[5].strip()
        if app == '${component_job_name}':
            is_running = True
            id = fields[0].strip()
            break

if is_running == True:
    print 'app is running, killing it...'
    subprocess.check_output(['yarn','application','-kill',id])
