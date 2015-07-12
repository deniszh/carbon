"""Copyright 2009 Chris Davis

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import time, os
import threading
from operator import itemgetter
from random import choice
from collections import defaultdict

from carbon.conf import settings
from carbon import events, log
from carbon.pipeline import Processor


def by_timestamp((timestamp, value)):  # useful sort key function
  return timestamp


class CacheFeedingProcessor(Processor):
  plugin_name = 'write'

  def process(self, metric, datapoint):
    MetricCache.store(metric, datapoint)
    return Processor.NO_OUTPUT


class DrainStrategy(object):
  """Implements the strategy for writing metrics.
  The strategy chooses what order (if any) metrics
  will be popped from the backing cache"""
  def __init__(self, cache):
    self.cache = cache

  def choose_item(self):
    raise NotImplemented


class NaiveStrategy(DrainStrategy):
  """Pop points in an unordered fashion."""
  def __init__(self, cache):
    super(NaiveStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        metric_names = self.cache.keys()
        while metric_names:
          yield metric_names.pop()

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class MaxStrategy(DrainStrategy):
  """Always pop the metric with the greatest number of points stored.
  This method leads to less variance in pointsPerUpdate but may mean
  that infrequently or irregularly updated metrics may not be written
  until shutdown """
  def choose_item(self):
    metric_name, size = max(self.cache.items(), key=lambda x: len(itemgetter(1)(x)))
    return metric_name


class RandomStrategy(DrainStrategy):
  """Pop points randomly"""
  def choose_item(self):
    return choice(self.cache.keys())


class SortedStrategy(DrainStrategy):
  """ The default strategy which prefers metrics with a greater number
  of cached points but guarantees every point gets written exactly once during
  a loop of the cache """
  def __init__(self, cache):
    super(SortedStrategy, self).__init__(cache)

    def _generate_queue():
      while True:
        t = time.time()
        metric_counts = sorted(self.cache.counts, key=lambda x: x[1])
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("Sorted %d cache queues in %.6f seconds" % (len(metric_counts), time.time() - t))
        while metric_counts:
          yield itemgetter(0)(metric_counts.pop())

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class _MetricCache(defaultdict):
  """A Singleton dictionary of metric names and lists of their datapoints"""
  def __init__(self, strategy=None):
    self.lock = threading.Lock()
    self.size = 0
<<<<<<< HEAD
    self.strategy = None
    if strategy:
      self.strategy = strategy(self)
    super(_MetricCache, self).__init__(dict)
=======
    self.method = method
    if self.method == "sorted":
      self.queue = self.gen_queue()
    else:
      self.queue = False
    super(_MetricCache, self).__init__(defaultfactory)

  def gen_queue(self):
    while True:
      t = time.time()
      queue = sorted(self.counts, key=lambda x: x[1])
      if settings.LOG_CACHE_QUEUE_SORTS:
        log.msg("Sorted %d cache queues in %.6f seconds" % (len(queue), time.time() - t))
      while queue:
        yield queue.pop()[0]

  def shutdown(self):
    # change to simple dequeuing system. generator will not be used any more
    self.method = "naive"

  def store(self, metric, datapoint):
    self.size += 1
    self[metric].append(datapoint)
    if self.isFull():
      log.msg("MetricCache is full: self.size=%d" % self.size)
      state.events.cacheFull()

  def isFull(self):
    # Short circuit this test if there is no max cache size, then we don't need
    # to do the someone expensive work of calculating the current size.
    return settings.MAX_CACHE_SIZE != float('inf') and self.size >= settings.MAX_CACHE_SIZE

  def pop(self, metric=None):
    if not self:
      raise KeyError(metric)
    elif not metric and self.method == "max":
      metric = max(self.items(), key=lambda x: len(x[1]))[0]
      datapoints = (metric, super(_MetricCache, self).pop(metric))
    elif not metric and self.method == "naive":
      datapoints = self.popitem()
    elif not metric and self.method == "sorted":
      metric = self.queue.next()
      # Save only last value for each timestamp
      popped = super(_MetricCache, self).pop(metric)
      ordered = sorted(dict(popped).items(), key=lambda x: x[0])
      datapoints = (metric, deque(ordered))
    self.size -= len(datapoints[1])
    return datapoints
>>>>>>> 2e63666... Add 'write cache to disk' feature

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

<<<<<<< HEAD
  @property
  def is_full(self):
    if settings.MAX_CACHE_SIZE == float('inf'):
      return False
    else:
      return self.size >= settings.MAX_CACHE_SIZE

  def _check_available_space(self):
    if state.cacheTooFull and self.size < settings.CACHE_SIZE_LOW_WATERMARK:
      log.msg("MetricCache below watermark: self.size=%d" % self.size)
      events.cacheSpaceAvailable()

  def drain_metric(self):
    """Returns a metric and it's datapoints in order determined by the
    `DrainStrategy`_"""
    if not self:
      return (None, [])
    if self.strategy:
      metric = self.strategy.choose_item()
    else:
      # Avoid .keys() as it dumps the whole list
      metric = self.iterkeys().next()
    return (metric, self.pop(metric))

  def get_datapoints(self, metric):
    """Return a list of currently cached datapoints sorted by timestamp"""
    return sorted(self.get(metric, {}).items(), key=by_timestamp)

  def pop(self, metric):
    with self.lock:
      datapoint_index = defaultdict.pop(self, metric)
      self.size -= len(datapoint_index)
    self._check_available_space()

    return sorted(datapoint_index.items(), key=by_timestamp)

  def store(self, metric, datapoint):
    timestamp, value = datapoint
    if timestamp not in self[metric]:
      # Not a duplicate, hence process if cache is not full
      if self.is_full:
        log.msg("MetricCache is full: self.size=%d" % self.size)
        events.cacheFull()
      else:
        with self.lock:
          self.size += 1
          self[metric][timestamp] = value
    else:
      # Updating a duplicate does not increase the cache size
      self[metric][timestamp] = value
=======
  def loadPersistMetricCache(self):
    persist_file = "%s/%s" % (settings.LOG_DIR, settings.CACHE_PERSIST_FILE)
    if not os.path.isfile(persist_file):
      return
    log.msg("Loading cache from persistant file : %s" % persist_file)

    t = time.time()
    try:
      queues = 0
      size = 0
      persist = open(persist_file, "r")
      for line in persist:
        try:
          fields = line.split(" ")
          metric = fields.pop(0)
          if len(fields) == 0:
            continue
          queues += 1
          while len(fields):
            value = float(fields.pop(0))
            timestamp = float(fields.pop(0))
            size += 1
            datapoint = (float(timestamp), float(value))
            self.store(metric, datapoint)
        except Exception, e:
          log.msg("Persisted cache : invalid data : %s" % line)
      persist.close()
      log.msg("Loaded persisted cache in %.2f seconds (metrics:%d queues:%d filesize:%d)" % (time.time() - t, size, queues, os.path.getsize(persist_file)))
    except Exception, e:
      log.err()
      log.msg("Unable to load persisted cache : %s" % persist_file)
      persist.close()

  def savePersistMetricCache(self, flush = False):
    started = time.time()
    metrics = self.items()
    persist_file = "%s/%s" % (settings.LOG_DIR, settings.CACHE_PERSIST_FILE)
    persist_file_tmp = "%s.tmp" % persist_file
    try:
      if os.path.isfile(persist_file):
        mtime = os.path.getmtime(persist_file)

        # try to protect from too many API calls (unless shutdown)
        if time.time() - mtime <= 60 and flush == False:
          log.msg("Cancel persist cache task (min interval : 60 sec)")
          return
    except OSError:
      log.err("Failed to get mtime of %s" % persist_file)
      return

    log.msg("Starting to save cache to persistant file : %s" % persist_file)

    if os.path.isfile(persist_file_tmp):
      os.unlink(persist_file_tmp)

    try:
      persist = open(persist_file_tmp, "w")
      queues = len(metrics)
      size = 0

      # do not pop metrics unless shutdown mode
      if flush == False:
        for metric, datapoints in metrics:
          size += len(datapoints)
          points = ""
          for datapoint in datapoints:
            points += " %s %s" % (datapoint[1], int(datapoint[0]))
          persist.write("%s%s\n" % (metric, points))
      else:
        while MetricCache:
          (metric, datapoints) = self.pop()
          size += len(datapoints)
          points = ""
          for datapoint in datapoints:
            points += " %s %s" % (datapoint[1], int(datapoint[0]))
          persist.write("%s%s\n" % (metric, points))

      persist.close()

      if os.path.isfile(persist_file):
        os.unlink(persist_file)

      os.rename(persist_file_tmp, persist_file)
    except Exception, e:
      log.msg("Error : unable to create persist file %s (%s)" % (persist_file, e))
      log.err()
      persist.close()
      return

    stopped = time.time()
    if flush == False:
      from carbon import instrumentation
      instrumentation.set('persist.queues', queues)
      instrumentation.set('persist.size', size)
      instrumentation.set('persist.fileSize', os.path.getsize(persist_file))
      instrumentation.set('persist.fileGeneration', stopped-started)
    log.msg("Persisted cache saved in %.2f seconds (metrics:%d queues:%d filesize:%d)" % (stopped-started, size, queues, os.path.getsize(persist_file)))

>>>>>>> 2e63666... Add 'write cache to disk' feature


# Initialize a singleton cache instance
write_strategy = None
if settings.CACHE_WRITE_STRATEGY == 'naive':
  write_strategy = NaiveStrategy
if settings.CACHE_WRITE_STRATEGY == 'max':
  write_strategy = MaxStrategy
if settings.CACHE_WRITE_STRATEGY == 'sorted':
  write_strategy = SortedStrategy
if settings.CACHE_WRITE_STRATEGY == 'random':
  write_strategy = RandomStrategy

MetricCache = _MetricCache(write_strategy)

# Avoid import circularities
from carbon import state
