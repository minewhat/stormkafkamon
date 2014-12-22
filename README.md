# stormkafkamon

Dumps state of Storm Kafka consumer spouts, showing how far behind each is behind,
relative to the Kafka partition it is consuming.

Check the "example" file for some sample output. This tool could be used to perform
simple monitoring of spout throughput.

Tested against Kafka 0.8.0 and Storm 0.9.0.1 running on Ubuntu 12.04
(using the Kafka spout from wurstmeister/storm-kafka-0.8-plus).

## Install

After cloning, run `pip install stormkafkamon`, or just

```
pip install https://github.com/otoolep/stormkafkamon/zipball/master
```

If you prefer, you can setup a virtualenv and install all the dependencies into it directly.

    virtualenv ~/.virtualenv/stormkafkamon
    ~/.virtualenv/stormkafkamon/bin/python setup.py install

## Usage

The entry-point is `monitor.py`. You probably know the Zookeeper host and topology name already,
but the `--spoutroot` is less obvious unless you've poked around inside Storm's entries in Zookeeper
quite a bit. This is where Storm stores information on the current offsets. I found this in the
`/transactional/{myspout}/user/` node for Zookeeper 3.4.5 and Storm 0.9.0.1.

For example, if your Zookeeper runs on `zoo01`, your topology is `stats` and your spout is named `spout1`:

    ./stormkafkamon/monitor.py --zserver zoo01 --topology stats --spoutroot /transactional/spout1/user/ --friendly

If you want to be fancy, combine this with /usr/bin/watch to have it automatically update every few seconds.

## Workflow

The code iterates through all Spout entries in Zookeeper, and retrieves all details. It then
contacts each Kafka broker listed in those details, and queries for the earliest available
offset, and latest, of each partition. This allows it to display the details shown in the example.

