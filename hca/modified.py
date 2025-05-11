import fdb, random, struct, threading

class AllocatorTransactionState:
    def __init__(self):
        self.lock = threading.Lock()

class HighContentionAllocator(object):
    def __init__(self, subspace):
        self.counters = subspace[0]
        self.recent = subspace[1]
        self.lock = threading.Lock()

    @fdb.transactional
    def allocate(self, tr):
        """Returns a byte string that
        1) has never and will never be returned by another call to this
           method on the same subspace
        2) is nearly as short as possible given the above
        """

        # Get transaction-local state
        if not hasattr(tr, "__fdb_directory_layer_hca_state__"):
            with self.lock:
                if not hasattr(tr, "__fdb_directory_layer_hca_state__"):
                    tr.__fdb_directory_layer_hca_state__ = AllocatorTransactionState()

        tr_state = tr.__fdb_directory_layer_hca_state__

        with tr_state.lock:
            while True:
                [start] = [
                    self.counters.unpack(k)[0]
                    for k, _ in tr.snapshot.get_range(
                        self.counters.range().start,
                        self.counters.range().stop,
                        limit=1,
                        reverse=True,
                    )
                ] or [0]

                window_advanced = False
                while True:
                    if window_advanced:
                        del tr[self.counters : self.counters[start]]
                        tr.options.set_next_write_no_write_conflict_range()
                        del tr[self.recent : self.recent[start]]

                    # Increment the allocation count for the current window
                    tr.add(self.counters[start], struct.pack("<q", 1))
                    count = tr.snapshot[self.counters[start]]

                    if count != None:
                        count = struct.unpack("<q", bytes(count))[0]
                    else:
                        count = 0

                    window = self._window_size(start)
                    if count * 2 < window:
                        break

                    start += window
                    window_advanced = True

                while True:
                    # As of the snapshot being read from, the window is less than half
                    # full, so this should be expected to take 2 tries.  Under high
                    # contention (and when the window advances), there is an additional
                    # subsequent risk of conflict for this transaction.
                    candidate = random.randrange(start, start + window)

                    latest_counter = tr.snapshot.get_range(
                        self.counters.range().start,
                        self.counters.range().stop,
                        limit=1,
                        reverse=True,
                    )
                    candidate_value = tr[self.recent[candidate]]
                    tr.options.set_next_write_no_write_conflict_range()
                    tr[self.recent[candidate]] = b""

                    latest_counter = [self.counters.unpack(k)[0] for k, _ in latest_counter]
                    if len(latest_counter) > 0 and latest_counter[0] > start:
                        break

                    if candidate_value == None:
                        tr.add_write_conflict_key(self.recent[candidate])
                        return fdb.tuple.pack((candidate,))

    def _window_size(self, start):
        # Larger window sizes are better for high contention, smaller sizes for
        # keeping the keys small.  But if there are many allocations, the keys
        # can't be too small.  So start small and scale up.  We don't want this
        # to ever get *too* big because we have to store about window_size/2
        # recent items.
        if start < 255:
            return 64
        if start < 65535:
            return 1024
        return 8192
