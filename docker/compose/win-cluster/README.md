# Simple docker compose setup for 3 node cluster

Note: Please remember to put *license.env* file in this directory. It should contain a line setting `RAVEN_License environment variable containing license information e.g.
```
RAVEN_License={"Id": "LICENSEID", "Name": "Testing", "Keys": [ ... ]}
```

## Create and run cluster
```
.\run.ps1 [-DontSetupCluster] [-StartBrowser]
-DontSetupCluster - just create nodes without setting them up
-StartBrowser - launch RavenDB studio in the browser 
```

## Destroy cluster
```
.\destroy.ps1
```
