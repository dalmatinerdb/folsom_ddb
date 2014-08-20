******
Readme
******

A storage plugin for folsom that stores data into `DalmatinerDB <https://dalmatiner.io>`_, metric collection can be triggered on a given interval.

Configuration
=============

A `cuttlefish <https://github.com/basho/cuttlefish>`_ schema file is provided in the `priv` directory otherwise the following app.config parameters are valid:

enabled
    `true` or `false`, enables or disables the entire collection, can be used to include it in systems that do not run DalmatinerDB causing 0 overhead.

ip
    `{Host, Port}` the Host and Port of the DalmatinerDB instance to send to.

bucket
    A string (not binary) of the bucket to use to store data.

prefix
    A string (not binary), the prefix to ad to each metric, the nodename is NOT automatically included.

buffer_size
     The approximitely send buffer size, please keep in mind that it is
     not a hard limit so select it a decent bit over the receive buffer of
     DalmatinerDB.

     Especially with histograms the buffer size can be overstepped sicne it is
     checked only after each metric.

interval
    Time between collection runs in milliseconds, 1000 (1 second) is a good number.

vm_metrics
   `true` or `false`, enable colection of VM metrics.
