@echo off
docker run -it -v "%CD%:/opt/data" --name wikiextract --rm python /bin/bash