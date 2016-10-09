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

import re
import random
from carbon.conf import settings
from carbon.regexlist import FlushList, ForceFlushList
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


class TunedStrategy(DrainStrategy):
  """tuned - Metrics will be flushed from the cache to disk in a tuned way."""
  def __init__(self, cache):
    super(TunedStrategy, self).__init__(cache)

    def _generate_queue(self):
      while True:
        start = time.time()
        t = time.time()
        g_count = 0
        g_size = 0
        queue = sorted(self.counts, key=lambda x: x[1])
        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg("[tuned#1] sorted %d queues in %.2f seconds" % (
          len(queue), time.time() - t))

        # parse config
        m = re.search("^(?P<val1>\d+(?:\.\d+)?)(?P<unit1>[%s]?)$",
                      str(settings.CACHE_WRITE_TUNED_STRATEGY_LARGEST))
        if m:
          if m.group("unit1") == "s":
            timelimit1 = int(m.group("val1"))
            limit1 = -1
          elif m.group("unit1") == "%":
            timelimit1 = -1
            limit1 = len(queue) * float(m.group("val1")) / 100
          else:
            timelimit1 = -1
            limit1 = int(m.group("val1"))
        else:
          limit1 = len(queue) * 0.01
          timelimit1 = -1

        m = re.search("^(?P<val2>\d+(?:\.\d+)?)(?P<unit2>[%s]?)$",
                      str(settings.CACHE_WRITE_TUNED_STRATEGY_RANDOM))
        if m:
          if m.group("unit2") == "s":
            timelimit2 = int(m.group("val2"))
            limit2 = -1
          elif m.group("unit2") == "%":
            timelimit2 = -1
            limit2 = len(queue) * float(m.group("val2")) / 100
          else:
            timelimit2 = -1
            limit2 = int(m.group("val2"))
        else:
          limit2 = 0
          timelimit2 = 60

        m = re.search("^(?P<val3>\d+(?:\.\d+)?)(?P<unit3>[%s]?)$",
                      str(settings.CACHE_WRITE_TUNED_STRATEGY_OLDEST))
        if m:
          if m.group("unit3") == "s":
            timelimit3 = int(m.group("val3"))
            limit3 = -1
          elif m.group("unit3") == "%":
            timelimit3 = -1
            limit3 = len(queue) * float(m.group("val3")) / 100
          else:
            timelimit3 = -1
            limit3 = int(m.group("val3"))
        else:
          limit3 = 0
          timelimit3 = 100

        # Step 1 : got largest queues (those with lots of metrics)
        if limit1 > 0 or timelimit1 > 0:
          t = time.time()
          count = 0
          size = 0
          len1 = 0
          len2 = 0
          while queue:
            if (limit1 != -1 and count >= limit1) or \
                    (timelimit1 != -1 and time.time() - t >= timelimit1):
              break
            metric = queue.pop()
            count += 1
            size += len(self[metric[0]])
            if count == 1:
              len1 = metric[1]
            len2 = metric[1]
            yield metric[0]
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg(
              "[tuned#1] written %d queues/%d metrics in %.2f seconds \
              (%.2f queues/sec %.2f metrics/sec) \
              (more numerous metrics/queue : %d -> %d)" % (
               count, size, time.time() - t, count / (time.time() - t),
               size / (time.time() - t), len1, len2))
          g_size += size
          g_count += count

        # Step 2 : got random queues
        if limit2 > 0 or timelimit2 > 0:
          t = time.time()
          random.shuffle(queue)
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg("[tuned#2] shuffled %d queues in %.2f seconds" % (
            len(queue), time.time() - t))

          t = time.time()
          count = 0
          size = 0
          while queue:
            if (limit2 != -1 and count >= limit2) or \
                    (timelimit2 != -1 and time.time() - t >= timelimit2):
              break
            metric = queue.pop()
            count += 1
            size += len(self[metric[0]])
            yield metric[0]
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg(
              "[tuned#2] written %d queues/%d metrics in %.2f seconds \
              (%.2f queues/sec %.2f metrics/sec) (random)" %
              (count, size, time.time() - t, count / (time.time() - t),
               size / (time.time() - t)))
          g_size += size
          g_count += count

        # Step 3 : got oldest queues (those with oldest metrics)
        if limit3 > 0 or timelimit3 > 0:
          t = time.time()
          ordered = sorted(self.oldest, key=lambda x: x[1], reverse=True)
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg("[tuned#3] sorted %d queues in %.2f seconds" %
                    (len(queue), time.time() - t))

          t = time.time()
          count = 0
          size = 0
          ts1 = 0
          ts2 = 0
          while ordered:
            if (limit3 != -1 and count >= limit3) or \
              (timelimit3 != -1 and time.time() - t >= timelimit3):
                break
            metric = ordered.pop()
            count += 1
            size += len(self[metric[0]])
            if count == 1:
              ts1 = time.time() - metric[1]
            ts2 = time.time() - metric[1]
            yield metric[0]
          if settings.LOG_CACHE_QUEUE_SORTS:
            log.msg(
              "[tuned#3] written %d queues/%d metrics in %.2f seconds \
              (%.2f queues/sec %.2f metrics/sec) \
              (oldest : %d sec -> %d sec late)" %
              (count, size, time.time() - t, count / (time.time() - t),
               size / (time.time() - t), int(ts1), int(ts2)))
          g_size += size
          g_count += count

        # Step 4 : got from flushlist (config file)
        if FlushList is not None and len(queue) != 0:
          t = time.time()
          count = 0
          size = 0
          for metric in self.items():
            if metric[0] in FlushList:
              count += 1
              size += len(self[metric[0]])
              yield metric[0]
          if count != 0:
            if settings.LOG_CACHE_QUEUE_SORTS:
              log.msg(
                "[tuned#4] written %d queues/%d metrics in %.2f seconds \
                (%.2f queues/sec %.2f metrics/sec) (flushlist)" %
                (count, size, time.time() - t, count / (time.time() - t),
                 size / (time.time() - t)))
          g_size += size
          g_count += count

        if settings.LOG_CACHE_QUEUE_SORTS:
          log.msg(
            "[tuned##] written %d queues/%d metrics in %.2f second \
            (%.2f queues/sec %.2f metrics/sec) (global)" %
            (g_count, g_size, time.time() - start,
             g_count / (time.time() - start), g_size / (time.time() - start)))

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class FlushStrategy(DrainStrategy):
  """ Special strategy for flushing metrics by regex using graphite-web api """
  def __init__(self, cache):
    super(FlushStrategy, self).__init__(cache)

    def _generate_queue(self):
      queue = self.items()
      if ForceFlushList is not None and len(queue) != 0:
        for metric in queue:
          if metric[0] in ForceFlushList:
            log.msg("flushing %s" % metric[0])
            yield metric[0]

    self.queue = _generate_queue()

  def choose_item(self):
    return self.queue.next()


class _MetricCache(defaultdict):
  """A Singleton dictionary of metric names and lists of their datapoints"""
  def __init__(self, strategy=None):
    self.lock = threading.Lock()
    self.size = 0
    self.strategy = None
    if strategy:
      self.strategy = strategy(self)
    self.save_strategy = strategy
    super(_MetricCache, self).__init__(dict)

  @property
  def counts(self):
    return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]

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

  def shutdown(self):
    # change to simple dequeuing system. generator will not be used any more
    self.strategy = NaiveStrategy

  @property
  def oldest(self):
    try:
      return [(metric, sorted(datapoints)[0][0]) for (metric, datapoints) in self.items()]
    except Exception, e:
      log.exception(e)
      log.msg("failed to create oldest list (%s)" % e)
      return []

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
      log.msg("Loaded persisted cache in %.2f seconds \
        (metrics:%d queues:%d filesize:%d)" %
              (time.time() - t, size, queues, os.path.getsize(persist_file)))
    except Exception, e:
      log.err()
      log.msg("Unable to load persisted cache : %s" % persist_file)
      persist.close()

  def savePersistMetricCache(self, flush=False):
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

    log.msg("Starting to save cache to persisted file : %s" % persist_file)

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
      instrumentation.set('persist.fileGeneration', stopped - started)
    log.msg("Persisted cache saved in %.2f seconds (metrics:%d queues:%d filesize:%d)" %
            (stopped - started, size, queues, os.path.getsize(persist_file)))

  def start_flush(self, metric = None):
    if self.strategy == FlushStrategy:
      return
    if metric == None:
      log.msg("[flush] no flush")
      return
    metrics_list = [metric]
    self.save_strategy = self.strategy
    ForceFlushList.load_from(metrics_list)
    self.strategy == FlushStrategy
    log.msg("[flush] starting flushing %s" % metrics_list)

  def stop_flush(self, abort=False):
    if self.strategy != FlushStrategy:
      return
    self.strategy = self.save_strategy
    if abort == True:
      log.msg("[flush] abort flush detected")
    log.msg("[flush] flushed %d queues in %.2f seconds" %
            (self.flushqueue_count, time.time() - self.flushqueue_start))

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
if settings.CACHE_WRITE_STRATEGY == 'tuned':
  write_strategy = TunedStrategy

MetricCache = _MetricCache(write_strategy)

# Avoid import circularities
from carbon import state