
# build image
docker build -t mina-telegram-subscriptions .

# tag image
docker tag mina-telegram-subscriptions makalfe/mina-telegram-subscriptions:latest

# push image
docker push makalfe/mina-telegram-subscriptions:latest
