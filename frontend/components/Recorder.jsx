import { useState, useEffect, useRef } from "react";
import { Animated, Pressable, View, Text, Alert } from "react-native";
import { FontAwesome } from "@expo/vector-icons";
import { styles } from "../styles/components.styles";
import Overlay from "./Overlay";
import Detected from "./Detected";
import { useProcess } from "../context/ProcessContext";
import * as DocumentPicker from "expo-document-picker";
import {
  useAudioRecorder,
  AudioModule,
  RecordingPresets,
  setAudioModeAsync,
  useAudioRecorderState,
  useAudioPlayer,
} from "expo-audio";

export default function Recorder() {
  const { addMatch } = useProcess();
  const [overlayVisible, setOverlayVisible] = useState(false);
  const [fileUri, setFileUri] = useState(null);
  const [playing, setPlaying] = useState(false);
  const [detectedData, setDetectedData] = useState(null);

  const scaleAnim = useRef(new Animated.Value(1)).current;
  const uploadAnim = useRef(new Animated.Value(1)).current;
  const timerRef = useRef(null);
  const recordTimeoutRef = useRef(null);

  const audioRecorder = useAudioRecorder(RecordingPresets.HIGH_QUALITY);
  const recorderState = useAudioRecorderState(audioRecorder);

  const player = useAudioPlayer(fileUri);

  useEffect(() => {
    (async () => {
      const status = await AudioModule.requestRecordingPermissionsAsync();
      if (!status.granted) {
        Alert.alert("Permission to access microphone was denied");
      }

      await setAudioModeAsync({
        playsInSilentMode: true,
        allowsRecording: true,
      });
    })();
  }, []);

  // start recording with 0.5s delay to prevent misfire
  const handlePressIn = () => {
    recordTimeoutRef.current = setTimeout(() => {
      startRecording();
    }, 500);
  };

  const handlePressOut = () => {
    clearTimeout(recordTimeoutRef.current);
    if (recorderState.isRecording) stopRecording();
  };

  const startRecording = async () => {
    try {
      Animated.loop(
        Animated.sequence([
          Animated.timing(scaleAnim, {
            toValue: 1.2,
            duration: 500,
            useNativeDriver: true,
          }),
          Animated.timing(scaleAnim, {
            toValue: 1,
            duration: 500,
            useNativeDriver: true,
          }),
        ])
      ).start();

      await audioRecorder.prepareToRecordAsync();
      audioRecorder.record();

      let elapsedSec = 0;
      timerRef.current = setInterval(() => {
        elapsedSec++;
        if (elapsedSec >= 15) stopRecording();
      }, 1000);
    } catch (err) {
      console.error("Failed to start recording:", err);
    }
  };

  const stopRecording = async () => {
    try {
      scaleAnim.stopAnimation();
      scaleAnim.setValue(1);
      clearInterval(timerRef.current);

      await audioRecorder.stop();
      setFileUri(audioRecorder.uri);
      setOverlayVisible(true);
    } catch (err) {
      console.error("Failed to stop recording:", err);
    }
  };

  const handleFilePick = async () => {
    try {
      const result = await DocumentPicker.getDocumentAsync({
        type: "audio/mpeg",
        copyToCacheDirectory: true,
        multiple: false,
      });

      if (result.canceled) return;
      const file = result.assets[0];

      if (file.size > 10 * 1024 * 1024) {
        Alert.alert("File too large", "Maximum upload size is 10MB");
        return;
      }

      setFileUri(file.uri);
      setOverlayVisible(true);
    } catch (err) {
      console.error("File pick error:", err);
    }
  };

  const togglePlayPause = () => {
    if (!player) return;
    if (playing) player.pause();
    else player.play();
    setPlaying(!playing);
  };

  const handleCancelPreview = () => {
    if (player) {
      player.pause();
      player.seekTo(0);
    }
    setPlaying(false);
    setFileUri(null);
    setOverlayVisible(false);
  };

  const handleDetectSong = async () => {
    if (!fileUri) return;
    let blob;
    try {
      const blobResponse = await fetch(fileUri);
      blob = await blobResponse.blob();
    } catch (error) {
      console.error("Failed to fetch Blob data:", error);
      Alert.alert("Error", "Could not read the audio file data.");
      return;
    }

    const formData = new FormData();

    let fileName = "recorded_audio.mp3"; // idk if i should use a generic name or what

    formData.append("file", blob, fileName);

    try {
      const response = await fetch("http://127.0.0.1:8000/match", {
        method: "POST",
        body: formData,
      });

      if (response.ok) {
        const result = await response.json();
        console.log("Detection successful:", result);
        setOverlayVisible(false);
        setPlaying(false);
        setFileUri(null);
        if (result.status === "success" && result.result?.length > 0) {
          const best = result.result[0];
          const matchData = {
            ...best.song_details,
            confidence: best.confidence,
          };
          setDetectedData(matchData);
          addMatch(matchData);
        } else {
          Alert.alert("No Match", "No matching song found in the database.");
        }
      } else {
        const errorData = await response.json();
        console.error("Detection failed:", response.status, errorData);
        Alert.alert(
          "Error",
          `Detection failed. Details: ${JSON.stringify(errorData.detail)}`
        );
      }
    } catch (error) {
      console.error("API call error:", error);
      Alert.alert("Error", "Network error or failed request.");
    }
  };

  return (
    <View style={styles.recorderWrapper}>
      <Animated.View style={{ transform: [{ scale: scaleAnim }] }}>
        <Pressable onPressIn={handlePressIn} onPressOut={handlePressOut}>
          <Animated.View style={styles.recorderButton}>
            <FontAwesome name="microphone" size={48} color="#fff" />
          </Animated.View>
        </Pressable>
      </Animated.View>

      <Animated.View style={{ transform: [{ scale: uploadAnim }] }}>
        <Pressable
          onPressIn={() =>
            Animated.spring(uploadAnim, {
              toValue: 0.95,
              useNativeDriver: true,
            }).start()
          }
          onPressOut={() =>
            Animated.spring(uploadAnim, {
              toValue: 1,
              useNativeDriver: true,
            }).start()
          }
          onPress={handleFilePick}
          style={({ pressed }) => [
            styles.recorderUploadButton,
            pressed && styles.recorderUploadButtonPressed,
          ]}
        >
          <FontAwesome name="upload" size={22} color="#fff" />
          <Text style={styles.recorderUploadText}>Or Upload MP3</Text>
        </Pressable>
      </Animated.View>

      {/* overlay with playback */}
      {fileUri && (
        <Overlay visible={overlayVisible} onClose={handleCancelPreview}>
          <View style={styles.recorderOverlayContent}>
            <Text style={styles.recorderOverlayTitle}>Audio Preview</Text>

            <Pressable
              style={styles.recorderPlayPauseButton}
              onPress={togglePlayPause}
            >
              <FontAwesome
                name={playing ? "pause" : "play"}
                size={32}
                color="#fff"
              />
            </Pressable>

            <View style={styles.recorderOverlayBottomRow}>
              <Pressable
                style={[
                  styles.recorderOverlayButton,
                  { flex: 1, marginRight: 8 },
                ]}
                onPress={handleDetectSong}
              >
                <Text style={styles.recorderOverlayButtonText}>
                  Detect Song
                </Text>
              </Pressable>
              <Pressable
                style={[
                  styles.recorderOverlayButton,
                  styles.recorderOverlayCancelButton,
                  { flex: 1, marginLeft: 8 },
                ]}
                onPress={handleCancelPreview}
              >
                <Text style={styles.recorderOverlayButtonText}>Cancel</Text>
              </Pressable>
            </View>
          </View>
        </Overlay>
      )}

      {detectedData && (
        <Detected
          data={detectedData}
          onClose={() => setDetectedData(null)}
        />
      )}
    </View>
  );
}
