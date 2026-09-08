import { useState, useEffect } from "react";
import { View, Text, TouchableOpacity, ScrollView } from "react-native";
import { styles } from "../styles/components.styles";
import ListItemCollapsed from "./ListItemCollapsed";
import { MaterialIcons } from "@expo/vector-icons";

export default function List() {
  const [items, setItems] = useState([]);
  const [loading, setLoading] = useState(false);

  const handleRefresh = async () => {
    setLoading(true);
    try {
      const response = await fetch("http://127.0.0.1:8000/dashboard");
      const data = await response.json();
      setItems(data.data || []);
    } catch (error) {
      console.error(error);
      setItems([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    handleRefresh();
  }, []);

  const skeletonCount = 5;

  return (
    <View style={styles.listContainer}>
      {/* Header */}
      <View style={styles.listHeader}>
        <Text style={styles.listHeaderText}>Database</Text>
        <View style={styles.listHeaderButtons}>
          {/* Refresh Icon */}
          <TouchableOpacity
            style={styles.buttonSmall}
            onPress={handleRefresh}
            disabled={loading} // disable while loading
          >
            <MaterialIcons
              name="refresh"
              size={20}
              color="#fff"
              style={{
                transform: [{ rotate: loading ? "360deg" : "0deg" }],
                transitionDuration: loading ? "1000ms" : "0ms",
              }}
            />
          </TouchableOpacity>
        </View>
      </View>

      {/* List items */}
      <ScrollView showsVerticalScrollIndicator={false}>
        {loading ? (
          Array.from({ length: skeletonCount }).map((_, index) => (
            <ListItemCollapsed key={index} loading />
          ))
        ) : items.length > 0 ? (
          items.map((item, index) => (
            <ListItemCollapsed
              key={index}
              spotify_ID={item.spotify_ID}
              youtube_ID={item.youtube_ID}
              entry_count={item.entry_count}
            />
          ))
        ) : (
          <Text style={{ color: "#ccc", textAlign: "center", marginTop: 20 }}>
            No items available
          </Text>
        )}
      </ScrollView>
    </View>
  );
}
