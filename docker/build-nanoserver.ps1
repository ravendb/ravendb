param([switch]$Run, [switch]$Wait)

docker build .\ravendb-nanoserver -t ravendb;

if($LASTEXITCODE -eq 0 -and $Run) {
    if($Wait) {
        docker run -it --rm -p 8080:8080 -v db:c:/databases ravendb;
    } else {
        docker run -d -p 8080:8080 -v db:c:/databases ravendb;
        start-sleep -Seconds 5;
        docker ps -q | % { docker inspect  -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $_ } | % { "http://$($_):8080/" };
    }
}