# Kill and remove previous docker containers
docker-compose down
# Delete previous scrape outputs
sudo rm -f /vol_b/data/scrapy_cluster_data/data.db

# Build containers
docker-compose build
# Start up new containers
docker-compose up -d
# Scale up the crawler container to have 10 total copies
docker-compose scale crawler=10
