
import fdb, threading, time

fdb.api_version(730)

import hca

db = fdb.open()
db.clear_range(b'', b'\xff')

allocators = {
  'fdb': fdb.HighContentionAllocator(fdb.Subspace(("a",))),
  'new': hca.HighContentionAllocator(fdb.Subspace(("b",)))
}

transaction_count = 5
allocations_per_transaction = 5

results = { name: [] for name in allocators.keys() }

def run(allocator):
  result = set()

  @fdb.transactional
  def run_transaction(tr):
    transaction_result = []

    def allocate():
      transaction_result.append(allocator.allocate(tr))

    transaction_threads = [threading.Thread(target=allocate) for _ in range(0, allocations_per_transaction)]
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

  assert(len(result) == transaction_count * allocations_per_transaction)

  return end - start

for i in range(0, 11):
  for name, allocator in allocators.items():
    results[name].append(run(allocator))

for name in allocators.keys():
  for result in results[name]:
      print(f"${name}: " + str(round(result * 1000000) / 1000))
  print('')

for name in allocators.keys():
  print(f'{name}: {str(round(sum(results[name]) / len(results[name]) * 1000000 / 1000))}')

print("Done")
