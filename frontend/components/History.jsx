import React from "react";
import { View, Text, ScrollView } from "react-native";
import ListItem from "./ListItem";
import { useProcess } from "../context/ProcessContext";
import { styles } from "../styles/components.styles";

export default function History() {
  const { matchHistory } = useProcess();

  return (
    <View style={styles.historyContainer}>
      <Text style={styles.historyTitle}>Your Recent Matches</Text>
      <ScrollView
        showsVerticalScrollIndicator={false}
        contentContainerStyle={{ paddingBottom: 20 }}
      >
        {matchHistory.length > 0 ? (
          matchHistory.map((item, index) => (
            <ListItem key={index} data={item} />
          ))
        ) : (
          <Text style={{ color: "#666", textAlign: "center", marginTop: 20 }}>
            No recent matches. Record or upload audio to detect songs.
          </Text>
        )}
      </ScrollView>
    </View>
  );
}
