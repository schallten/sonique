import hashlib

def generate_hashes(peaks, track_id, fanout=7, max_time_delta=5):
    """Convert peaks into hashes by creating relationships between them.
    
    Individual peaks aren't very useful but relationships are unique in songs.
    Peaks should be pre-pruned before passing to this function."""
    hashes = []
    # Sorting by time so that process finds time based neighbours efficiently
    peaks_sorted = sorted(peaks, key=lambda x: x[1])

    # Use each peak as anchor
    for i, anchor in enumerate(peaks_sorted):
        anchor_freq, anchor_time, anchor_mag = anchor
        
        # Limit fanout - only process up to fanout neighbours
        neighbours_collected = 0
        for j in range(i + 1, len(peaks_sorted)):
            if neighbours_collected >= fanout:
                break
                
            target_freq, target_time, target_mag = peaks_sorted[j]
            # Setting time constraint because too far relationships are useless due to less accuracy
            if target_time - anchor_time > max_time_delta:
                break

            time_delta = target_time - anchor_time
            # Create hash from three values for uniqueness
            hash_int = create_hash(anchor_freq, target_freq, time_delta)
            # Store as tuple: (hash_int, anchor_time)
            hashes.append((hash_int, int(anchor_time)))
            neighbours_collected += 1

    return hashes, track_id  # Return track_id separately instead of storing with each hash

def create_hash(freq1, freq2, time_delta):
    """Create unique hash by combining three values into a single integer using bit packing."""
    
    # Constraints to avoid overflow or too large numbers
    freq1 = min(freq1, 2**10 - 1)  # 10 bits for frequency (under 1024 bins)
    freq2 = min(freq2, 2**10 - 1)  
    time_delta = min(time_delta, 2**8 - 1)  # 8 bits for time delta (usually small)

    # Bit packing: freq1(10 bits) | freq2(10 bits) | time_delta(8 bits)
    # Creates 2^28 unique possible combinations
    hash_int = (freq1 << 18) | (freq2 << 8) | time_delta
    
    return hex(hash_int)
