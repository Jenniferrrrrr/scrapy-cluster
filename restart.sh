# Kill and remove previous docker containers
docker-compose down
# Delete previous scrape outputs
sudo rm -f /vol_b/data/scrapy_cluster_data/data.db

# Build containers
docker-compose build
# Start up new containers, scaling to 10 copies of crawler container
docker-compose up -d --scale crawler=10
