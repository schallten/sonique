## makes peak
### as the name suggests
#### btw

import numpy as np
from scipy.ndimage import maximum_filter

Peak = tuple[float, float, float]


def extract_peaks(
    spectrogram_db: np.ndarray,
    magnitude_threshold: float = -25,
    neighborhood_size: int = 15,
) -> list[Peak]:
    """
    Extract identifiable peaks from dB-scaled spectrogram.
    WHY: We need stable audio landmarks that survive noise and compression.

    NOTE: magnitude_threshold is now in dB scale (-80 to 0), so we adjust the default
    """
    # WHY USE LOCAL MAXIMA: To find points that stand out from their surroundings
    # This finds points brighter than all neighbors in a defined area
    data_max = maximum_filter(spectrogram_db, neighborhood_size)

    # WHY COMPARE WITH LOCAL MAX: Identifies exact peak locations
    # Only points that ARE the local maximum get marked as potential peaks
    maxima_mask = (spectrogram_db == data_max)

    # WHY MAGNITUDE THRESHOLD: Filter out background noise
    # In dB scale, typical thresholds are between -30 to -10 dB
    # Lower values = more sensitive, higher values = more selective
    magnitude_mask = spectrogram_db > magnitude_threshold

    # WHY COMBINE BOTH MASKS: We want peaks that are both local maxima AND sufficiently bright
    peak_mask = maxima_mask & magnitude_mask

    # WHY GET COORDINATES: Convert from boolean mask to usable (freq_bin, time_frame, magnitude) tuples
    freq_bins, time_frames = np.where(peak_mask)
    magnitudes = spectrogram_db[freq_bins, time_frames]

    peaks: list[Peak] = list(zip(freq_bins, time_frames, magnitudes))

    # WHY FILTER FALSE PEAKS: Remove clusters where multiple points mark the same audio feature
    # This prevents duplicate hashes for the same musical event
    peaks = filter_false_peaks(peaks, spectrogram_db)

    # WHY SORT BY TIME: Makes subsequent hash generation sequential and efficient
    return sorted(peaks, key=lambda x: x[1])


def filter_false_peaks(
    peaks: list[Peak],
    spectrogram_db: np.ndarray,
    min_freq_distance: int = 5,
    min_time_distance: int = 3,
) -> list[Peak]:
    """
    Remove peaks that are too close to stronger peaks.
    WHY: Single audio events often create multiple nearby peaks in spectrogram.
    """
    filtered_peaks: list[Peak] = []

    for i, (f1, t1, mag1) in enumerate(peaks):
        is_valid = True

        # WHY CHECK AGAINST OTHER PEAKS: Find if current peak has stronger neighbor nearby
        for j, (f2, t2, mag2) in enumerate(peaks):
            # WHY SKIP SELF COMPARISON: Don't compare peak with itself
            if i != j and abs(f1 - f2) < min_freq_distance and abs(t1 - t2) < min_time_distance:
                # WHY KEEP STRONGER PEAK: The brighter peak better represents the audio feature
                # In dB scale, higher values = stronger peaks
                if mag2 > mag1:
                    is_valid = False
                    break

        # WHY ONLY ADD IF VALID: Ensure we keep only the most representative peak per audio feature
        if is_valid:
            filtered_peaks.append((f1, t1, mag1))

    return filtered_peaks