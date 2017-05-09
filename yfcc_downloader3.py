from queue import Queue
import urllib3
import certifi
import threading
import progressbar
import logging
import argparse
import os, sys

job_queue = Queue()

def _worker():
  while True:
    url, filepath = job_queue.get()
    if not os.path.exists(filepath):
      try:
        http = urllib3.PoolManager(cert_reqs = 'CERT_REQUIRED',
            ca_certs = certifi.where())
        r = http.request('GET', url, timeout = 60, retries = 3)
        with open(filepath, 'wb') as fout:
          fout.write(r.data)
      except Exception as err:
        if os.path.exists(filepath):
          os.remove(filepath)
        logging.warning('{}: {}: {}'.format(
            os.path.basename(filepath).split('.')[0], type(err).__name__, err))
    job_queue.task_done()

def download(gen, num_parallel):
  '''
  gen: generator that produce (url, filepath)
  num_parallel: number of jobs running in parallel
  '''
  logging.basicConfig(format='%(asctime)s %(message)s', 
      filename = '{}.log'.format(__file__), level = logging.WARNING)
  bar = progressbar.ProgressBar(max_value = gen.get_total_num())

  for i in range(num_parallel):
    t = threading.Thread(target = _worker)
    t.daemon = True
    t.start()

  stop = False
  for i, data in enumerate(gen):
    wait = True
    while wait:
      try:
        wait = job_queue.qsize() >= num_parallel
      except KeyboardInterrupt:
        print('Stopped.')
        wait = True
        break
    if wait:
      break
    job_queue.put(data)
    bar.update(i)

  print('Waiting for remaining jobs...')
  job_queue.join()
  print('Finished.')

class YFCC():
  '''YFCC generator'''
  def __init__(self, meta_path, save_rpath):
    self.save_rpath = save_rpath
    self.base_url = ('https://multimedia-commons.s3-us-west-2.amazonaws.com/'
        'data/videos/mp4')
    self.dataset = []
    with open(meta_path, 'r') as fin:
      for line in fin:
        line = line.split()
        self.dataset.append(line)

  def __iter__(self):
    self.i = 0
    return self

  def __next__(self):
    if self.i >= len(self.dataset):
      raise StopIteration
    data = self.dataset[self.i]
    # get url
    hval = data[2]
    url = os.path.join(self.base_url, hval[:3], hval[3: 6], 
        '{}.mp4'.format(hval))
    # get save path
    filepath = os.path.join(self.save_rpath, 
        '1{:03d}'.format((self.i) // 1000))
    if not os.path.exists(filepath):
      os.mkdir(filepath)
    filepath = os.path.join(filepath, '{}.mp4'.format(data[1]))
    self.i += 1
    return url, filepath

  def get_total_num(self):
    return len(self.dataset)

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'YFCC downloader 3')
  parser.add_argument('meta_path', help = 'Meta data path')
  parser.add_argument('save_rpath', help = 'Saving root path')
  parser.add_argument('-p', '--parallel', type = int, default = 5,
      help = 'Saving root path')
  args = parser.parse_args()

  gen = YFCC(args.meta_path, args.save_rpath)
  download(gen, args.parallel)
