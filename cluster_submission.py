import subprocess
import os

def submit_job(job_directory,
               job_script):
    """

    Parameters
    ----------
    job_directory
    job_script

    Returns
    -------

    Notes
    ---------

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/cluster_submission.py

    """

    submission_string = ' '.join([
        'cd',
        job_directory,
        ';',
        'module load global/cluster >>/dev/null 2>&1; qsub -q low.q ',
        job_script,
    ])


    print(submission_string)

    submission = subprocess.Popen(submission_string,
                                  shell=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)

    out, err = submission.communicate()

    out = out.decode('ascii')
    print('\n')
    print(out)
    print('\n')
    if err:
        err = err.decode('ascii')
        print('\n')
        print(err)
        print('\n')


def write_job(execute_directory, job_directory, job_filename, job_command):

    """

    Parameters
    ----------
    execute_directory
    job_directory
    job_filename
    job_command

    Returns
    -------

    Notes
    ---------

    Skeleton code adapted from working version for the formulatrix
    pipeline at diamond:

    https://github.com/xchem/formulatrix_pipe/blob/master/cluster_submission.py

    """

    directory = os.getcwd()
    os.chdir(job_directory)
    job_script = '''#!/bin/bash
    cd %s
    %s
    ''' % (execute_directory, job_command)

    output = os.path.join(job_directory, job_filename)

    f = open(output, 'w')
    f.write(job_script)
    os.chdir(directory)