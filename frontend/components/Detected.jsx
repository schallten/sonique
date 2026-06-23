import { View, Text, Image, TouchableOpacity, Linking } from "react-native";
import { FontAwesome } from "@expo/vector-icons";
import Overlay from "./Overlay";
import { styles } from "../styles/components.styles";

export default function Detected({ data }) {
  const confidencePercent = Math.round(data.confidence);

  const openSpotify = () => {
    Linking.openURL(`https://open.spotify.com/track/${data.spotify_ID}`);
  };

  const openYouTube = () => {
    Linking.openURL(`https://www.youtube.com/watch?v=${data.youtube_ID}`);
  };

  return (
    <Overlay visible={true} onClose={() => {}}>
      <View style={styles.detectedContainer}>
        <Text style={styles.detectedConfidence}>
          Confidence: {confidencePercent}%
        </Text>

        <Image source={{ uri: data.cover }} style={styles.detectedCover} />

        <Text style={styles.detectedTitle}>{data.title}</Text>
        <Text style={styles.detectedArtist}>{data.artists}</Text>
        <Text style={styles.detectedAlbum}>{data.album_name}</Text>

        <View style={styles.detectedButtonsRow}>
          <TouchableOpacity
            style={[styles.detectedButton, styles.detectedSpotify]}
            onPress={openSpotify}
          >
            <FontAwesome name="spotify" size={22} color="#fff" />
            <Text style={styles.detectedButtonText}>Spotify</Text>
          </TouchableOpacity>

          <TouchableOpacity
            style={[styles.detectedButton, styles.detectedYouTube]}
            onPress={openYouTube}
          >
            <FontAwesome name="youtube-play" size={22} color="#fff" />
            <Text style={styles.detectedButtonText}>YouTube</Text>
          </TouchableOpacity>
        </View>
      </View>
    </Overlay>
  );
}
