
import fdb, threading, time, hca

fdb.api_version(730)
db = fdb.open()
db.clear_range(b'', b'\xff')

fdb_allocator = fdb.HighContentionAllocator(fdb.Subspace(("b",)))
new_allocator = hca.HighContentionAllocator(fdb.Subspace(("a",)))

scale = 5
result = set()

results = []

def run_thread(allocator):
  @fdb.transactional
  def run_transaction(tr):
    transaction_result = []

    def allocate():
      transaction_result.append(allocator.allocate(tr))

    transaction_threads = [threading.Thread(target=allocate) for _ in range(0, scale)]
    for thread in transaction_threads:
      thread.start()
    for thread in transaction_threads:
      thread.join()

    return transaction_result

  for transaction_result in run_transaction(db):
    result.add(transaction_result)

for i in range(0, 10):
  start = time.time()

  threads = [threading.Thread(target=run_thread, args=(new_allocator if i % 2 else fdb_allocator,)) for _ in range(0, scale)]

  for thread in threads:
    thread.start()

  for thread in threads:
    thread.join()

  end = time.time()

  if (len(result) != scale * scale):
    print(result)
    print(len(result))

  assert(len(result) == scale * scale)
  result = set()
  results.append(end - start)

for i in range(1, len(results) // 2):
    print("fdb: " + str(round((results[i * 2]) * 1000000) / 1000))

for i in range(1, len(results) // 2):
    print("new: " + str(round((results[i * 2 + 1]) * 1000000) / 1000))

print("Done");
