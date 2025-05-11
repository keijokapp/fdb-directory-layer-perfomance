
import fdb, threading, time, math
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
fdb.api_version(730)

import hca.original as originalHca
import hca.modified as modifiedHca
import hca.new as newHca

db = fdb.open()
db.clear_range(b'', b'\xff')

allocators = {
  'original': originalHca.HighContentionAllocator(fdb.Subspace(("a",))),
  # 'modified': modifiedHca.HighContentionAllocator(fdb.Subspace(("b",))),
  'new': newHca.HighContentionAllocator(fdb.Subspace(("c",)))
}

# sample_count = 25
# transaction_count = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30]
# allocation_count = [1, 2, 3, 4, 5, 10, 15, 20, 25, 30]
sample_count = 1
transaction_count = [1, 3, 9, 17]
allocation_count = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

def run(allocator, transaction_count, allocation_count):
  db.clear_range(b'', b'\xff')
  result = set()

  @fdb.transactional
  def run_transaction(tr):
    transaction_result = []

    def allocate():
      transaction_result.append(allocator.allocate(tr))

    transaction_threads = [threading.Thread(target=allocate) for _ in range(0, allocation_count)]
    for thread in transaction_threads:
      thread.start()
    for thread in transaction_threads:
      thread.join()

    return transaction_result

  def run_thread():
    for transaction_result in run_transaction(db):
      result.add(transaction_result)

  threads = [threading.Thread(target=run_thread) for _ in range(0, transaction_count)]

  start = time.time()

  for thread in threads:
    thread.start()
  for thread in threads:
    thread.join()

  end = time.time()

  assert(len(result) == transaction_count * allocation_count)

  return end - start

results = { name: { (tc, apt): [] for apt in allocation_count for tc in transaction_count } for name in allocators.keys() }

for i in range(-1, sample_count):
  print(f"sample {i}")
  for name, allocator in allocators.items():
    for (tc, apt) in results[name].keys():
      result = run(allocator, tc, apt)
      if (i >= 0):
        results[name][(tc, apt)].append(result)

###################################################################

plot_data = {}

for name in allocators.keys():
  data = []

  for y in range(min(allocation_count), max(allocation_count) + 1):
    arr = [];
    for x in range(min(transaction_count), max(transaction_count) + 1):
      if (x, y) in results[name]:
        arr.append(sum(results[name][(x, y)]))
      else:
        arr.append(math.nan)
    data.append(arr)

  plot_data[name] = data

###################################################################

_, axs = plt.subplots(1, len(allocators) + 1)

for i, name in enumerate(allocators.keys()):
  ax = axs[i]
  im = ax.imshow(plot_data[name], origin='lower', extent=(min(transaction_count) - 0.5, max(transaction_count) + 0.5, min(allocation_count) - 0.5, max(allocation_count) + 0.5))
  ax.set_xlabel('Transaction count')
  ax.set_ylabel('Allocation count per transaction')
  ax.set_xticks(ticks=transaction_count)
  ax.set_yticks(ticks=allocation_count)
  plt.colorbar(im, label="seconds", ax=ax)
  print('')


####################################################################

data = [[c - d for c, d in zip(a, b)] for a, b in zip(plot_data['original'], plot_data['new'])]
ax = axs[len(allocators)]
im = ax.imshow(data, origin='lower', extent=(min(transaction_count) - 0.5, max(transaction_count) + 0.5, min(allocation_count) - 0.5, max(allocation_count) + 0.5), cmap=LinearSegmentedColormap.from_list('RedGreen', ['red', 'green']))
ax.set_xlabel('Transaction count')
ax.set_ylabel('Allocation count per transaction')
plt.colorbar(im, label="diff in seconds", ax=ax)

for y, a in enumerate(data):
  for x, value in enumerate(a):
    if not math.isnan(value):
      ax.annotate(str(round(value * 1000)), xy=(x+1, y+1), ha='center', va='center', color='white')

print('')


####################################################################

for y in range(1, max(allocation_count) + 1):
  arr = [];
  for x in range(1, max(transaction_count) + 1):
    for name in allocators.keys():
      print(f'{name} {(y, x)}: {str(round(sum(results[name][max(transaction_count), max(allocation_count)]) * 1000000) / 1000)}')
    print('')

plt.show()

print("Done")
