# TCP Streaming Source and Sink

## Example commands

`./tcp_source bid_events.parquet --address 0.0.0.0:10000 --schema bid --exp-name bid --system nes`

`./tcp_source auction_events.parquet --address 0.0.0.0:10001 --schema auction --exp-name auction --system default`

`./tcp_source person_events.parquet --address 0.0.0.0:10002 --schema person --system default`

## Docker

## Source
`docker run --rm --init --network=host \
  -v "$PWD/logs":/opt/tcp/logs \
  -v "$PWD/data:/data:ro" \
  maxhue/tcp-streaming \
  source /data/bid_events.parquet --schema bid --exp-name bid`

## Sink

`docker run --rm --init -it --network=host \
  -v "$PWD/logs":/opt/tcp/logs \
  maxhue/tcp-streaming \
  sink --exp-name test`
