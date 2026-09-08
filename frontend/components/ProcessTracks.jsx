import { useState } from "react";
import { useProcess } from "../context/ProcessContext";
import {
  Text,
  TouchableOpacity,
  ScrollView,
  View,
  TextInput,
} from "react-native";
import Overlay from "./Overlay";
import { styles } from "../styles/components.styles";

export default function ProcessTracks() {
  const {
    loading,
    setLoading,
    responseData,
    setResponseData,
    responseOverlayVisible,
    setResponseOverlayVisible,
  } = useProcess();

  const [overlayVisible, setOverlayVisible] = useState(false);
  const [entries, setEntries] = useState([""]);

  const handleAddEntry = () => setEntries([...entries, ""]);
  const handleRemoveEntry = (index) =>
    setEntries(entries.filter((_, i) => i !== index));
  const handleChangeEntry = (text, index) => {
    const updated = [...entries];
    updated[index] = text;
    setEntries(updated);
  };

  const isValidLink = (urlString) => {
    if (!urlString.trim()) return false;
    const trimmed = urlString.trim();
    if (trimmed.includes("youtube.com") || trimmed.includes("youtu.be"))
      return true;
    if (trimmed.includes("spotify.com")) return true;
    return false;
  };

  const canProcess =
    entries.length > 0 &&
    entries.some((e) => e.trim()) &&
    entries.every((e) => e.trim() === "" || isValidLink(e));

  const parse_inputs = (entriesArray) => {
    const result = {
      track_id: [],
      album_id: [],
      playlist_id: [],
      youtube_url: "",
    };

    entriesArray.forEach((entry) => {
      if (!entry.trim()) return;
      const trimmed = entry.trim();

      if (trimmed.includes("youtube.com") || trimmed.includes("youtu.be")) {
        result.youtube_url = trimmed;
        return;
      }

      try {
        const url = new URL(trimmed);
        const parts = url.pathname.split("/").filter(Boolean);
        const type = parts[0];
        const id = parts[1];
        if (!id) result.track_id.push(trimmed);
        else if (type === "track") result.track_id.push(id);
        else if (type === "album") result.album_id.push(id);
        else if (type === "playlist") result.playlist_id.push(id);
      } catch {
        result.track_id.push(trimmed);
      }
    });

    return result;
  };

  const handleProcess = async () => {
    setOverlayVisible(false); // close overlay immediately
    setLoading(true); // disable main button
    try {
      const payload = parse_inputs(entries);
      const res = await fetch("http://127.0.0.1:8000/load", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });

      const data = await res.json();
      setResponseData(data);
      setResponseOverlayVisible(true); // show response overlay
    } catch (err) {
      console.error("Failed to process tracks:", err);
      setResponseData({ error: "Failed to process tracks" });
      setResponseOverlayVisible(true);
    } finally {
      setLoading(false); // re-enable main button
    }
  };

  return (
    <>
      <TouchableOpacity
        onPress={() => setOverlayVisible(true)}
        disabled={loading}
        style={{ opacity: loading ? 0.6 : 1 }}
      >
        <Text style={styles.processButton}>
          {loading ? "Processing..." : "Process Tracks"}
        </Text>
      </TouchableOpacity>

      {/* input */}
      <Overlay
        visible={overlayVisible}
        onClose={() => setOverlayVisible(false)}
      >
        <ScrollView
          contentContainerStyle={styles.overlayEntriesContainer}
          showsVerticalScrollIndicator={false}
        >
          {entries.map((entry, index) => (
            <View key={index} style={styles.overlayEntryRow}>
              <TextInput
                style={styles.overlayEntryText}
                value={entry}
                onChangeText={(text) => handleChangeEntry(text, index)}
                placeholder={`Entry ${index + 1}`}
                placeholderTextColor="#555"
              />
              <TouchableOpacity
                style={styles.overlayRemoveButton}
                onPress={() => handleRemoveEntry(index)}
              >
                <Text style={styles.overlayRemoveButtonText}>-</Text>
              </TouchableOpacity>
            </View>
          ))}
        </ScrollView>

        <View style={styles.overlayBottomButtons}>
          <TouchableOpacity
            style={[
              styles.overlayButton,
              (!canProcess || loading) && { opacity: 0.5 },
            ]}
            onPress={handleProcess}
            disabled={!canProcess || loading}
          >
            <Text style={styles.overlayButtonText}>Process</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={styles.overlayButton}
            onPress={handleAddEntry}
          >
            <Text style={styles.overlayButtonText}>+</Text>
          </TouchableOpacity>
          <TouchableOpacity
            style={[styles.overlayButton, styles.overlayCancelButton]}
            onPress={() => setOverlayVisible(false)}
          >
            <Text style={styles.overlayButtonText}>Cancel</Text>
          </TouchableOpacity>
        </View>
      </Overlay>

      {/* response */}
      <Overlay
        visible={responseOverlayVisible}
        onClose={() => setResponseOverlayVisible(false)}
      >
        <ScrollView
          contentContainerStyle={{ padding: 12 }}
          showsVerticalScrollIndicator={false}
        >
          <Text
            style={{
              color: "#fff",
              fontWeight: "700",
              fontSize: 18,
              marginBottom: 12,
            }}
          >
            {responseData?.message || "Response"}
          </Text>

          {responseData?.details && (
            <View style={{ marginBottom: 20 }}>
              <Text
                style={{ color: "#ccc", fontWeight: "600", marginBottom: 4 }}
              >
                Processed: {responseData.details.processed}
              </Text>
              <Text
                style={{ color: "#ccc", fontWeight: "600", marginBottom: 4 }}
              >
                Skipped : {responseData.details.skipped}
              </Text>
              <Text
                style={{ color: "#ccc", fontWeight: "600", marginBottom: 4 }}
              >
                Duration (s) : {responseData.details.duration}
              </Text>

              <Text
                style={{ color: "#ccc", fontWeight: "600", marginBottom: 4 }}
              >
                Average (s) : {responseData.details.average}
              </Text>
            </View>
          )}

          <TouchableOpacity
            style={[styles.overlayButton, { width: "100%" }]}
            onPress={() => setResponseOverlayVisible(false)}
          >
            <Text style={styles.overlayButtonText}>Close</Text>
          </TouchableOpacity>
        </ScrollView>
      </Overlay>
    </>
  );
}
