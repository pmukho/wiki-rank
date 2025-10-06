#!/bin/bash
#
# Download pagelinks and page dumps from wikimedia

cd "$(dirname "$0")"

# Ensure data directory exists
DATA_DIRECTORY="../data"

if [[ -d $DATA_DIRECTORY ]]; then
    echo "$DATA_DIRECTORY does exist"
else
    echo "Making $DATA_DIRECTORY"
    mkdir -p "$DATA_DIRECTORY"
fi

# Retrieve dumps as needed
PROTOCOL=https://
DOMAIN=dumps.wikimedia.your.org
WIKI=enwiktionary
YEAR=$(date +%Y)
MONTH=$(date +%m)
if [ "$(date +%d)" -ge 20 ]; then
	DAY=20;
else
	DAY=01;
fi
# DATE=$YEAR$MONTH$DAY
DATE=latest
BASE_URL="$PROTOCOL$DOMAIN/$WIKI/$DATE/$WIKI-$DATE"

PAGE_FILE="page.sql.gz"
PAGE_URL="$BASE_URL-$PAGE_FILE"

PAGELINKS_FILE="pagelinks.sql.gz"
PAGELINKS_URL="$BASE_URL-$PAGELINKS_FILE"

download_if_missing () {
    local url="$1"
    local target="$2"

    if [[ -f "$DATA_DIRECTORY/$target" && -s "$DATA_DIRECTORY/$target" ]]; then
        echo "$target does exist"
    else
        echo "Downloading from $url"

        wget -c -O "$DATA_DIRECTORY/$target" "$url"
        if [[ $? -eq 0 ]]; then
            echo "Download successful"
        else
            echo "Failed to download $target from $url"
        fi
    fi
}

download_if_missing "$PAGE_URL" "$PAGE_FILE"
download_if_missing "$PAGELINKS_URL" "$PAGELINKS_FILE"