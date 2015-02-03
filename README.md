# bing-bulk-geocoder
Python library to make bing bulk geocoding a wee bit easier.

Run it from the command line and give it a comma-separated file, one line per address, with the format:

ID,Address

It'll require your license key to be present in your env as BING_MAPS_API_KEY or it'll ask for it interactively.