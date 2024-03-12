# Author: K Geil
# Date: March 2024
# Description: Climate Data Store Parallel Download Demo. Illustrating how to use the Climate Data Store (CDS) API 
# to download data files in parallel from the CDS directly to HPC Orion. This example downloads files from the AgERA5 dataset.

# Prerequisites for running this script include: 
# 1) if it's your first time using the Climate Data Store, self register at the CDS registration page 
# (https://cds.climate.copernicus.eu/user/register?destination=%2F%23!%2Fhome) 
# 2) if it's your first time using the cdsapi for downloading, create a file called .cdsapirc containing the 
# cdsapi url and your key to your home directory. instructions for installing the CDS API key are at
# (https://cds.climate.copernicus.eu/api-how-to#install-the-cds-api-key). remember you can use the nano text 
# editor to create files at the command line.
# 3) create a conda environment with the cdsapi and dask packages installed if you don't already have this, 
# 4) use the srun commmand to move onto a development node using anywhere from 1-20 cores (e.g. --nodes=1 --ntasks=10 or -N 1 -n 10)
# 5) activate your conda environment
# 6) navigate to the directory where you've saved this script

# Run this script at the command line:
# python download_CDS_AgERA5_parallel.py


import cdsapi
import os
import dask
from dask.distributed import Client,LocalCluster

# user-specific variables
basedir='/path/to/our/shared/datasets/dir/'
outputdir=basedir+'temporary/AgERA5/'
year_first = 1990
year_last = 1999
ntasks=10  # make this the same as --ntasks (-n) from the srun command

# setup
if not os.path.exists(outputdir):
    os.makedirs(outputdir)

print('starting client...')
if __name__ == "__main__":
    nworkers=ntasks 
    cluster=LocalCluster(n_workers=nworkers,threads_per_worker=1) # a cluster where each thread is a separate process or "worker"
    client=Client(cluster)  # connect to your compute cluster
    client.wait_for_workers(n_workers=nworkers,timeout=10) # wait up to 10s for the cluster to be fully ready, error if not ready in 10s    

# function to grab 1 year of data 
def get_CDS_data(year):

    c = cdsapi.Client() # connect to Climate Data Store 

    # api call
    c.retrieve(
    'sis-agrometeorological-indicators',
    {
        'variable': '2m_temperature',
        'year': year,
        'month': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
        ],
        'day': [
            '01', '02', '03',
            '04', '05', '06',
            '07', '08', '09',
            '10', '11', '12',
            '13', '14', '15',
            '16', '17', '18',
            '19', '20', '21',
            '22', '23', '24',
            '25', '26', '27',
            '28', '29', '30',
            '31',
        ],
        'version': '1_1',
        'format': 'tgz',
        'statistic': '24_hour_maximum',
    },
    outputdir+'AgERA5_'+year+'.tar.gz')

# build your list of delayed compute tasks
tasklist=[]
for yyyy in range(year_first,year_last+1):
    tasklist.append(dask.delayed(get_CDS_data)(str(yyyy)))    

# execute tasks (downloads)    
dask.compute(*tasklist)
