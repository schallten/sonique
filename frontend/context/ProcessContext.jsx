import { createContext, useState, useContext } from "react";

const ProcessContext = createContext();

const MAX_HISTORY = 20;

export function ProcessProvider({ children }) {
  const [loading, setLoading] = useState(false);
  const [responseData, setResponseData] = useState(null);
  const [responseOverlayVisible, setResponseOverlayVisible] = useState(false);
  const [matchHistory, setMatchHistory] = useState([]);

  const addMatch = (match) => {
    setMatchHistory((prev) => {
      const updated = [match, ...prev];
      return updated.slice(0, MAX_HISTORY);
    });
  };

  return (
    <ProcessContext.Provider
      value={{
        loading,
        setLoading,
        responseData,
        setResponseData,
        responseOverlayVisible,
        setResponseOverlayVisible,
        matchHistory,
        addMatch,
      }}
    >
      {children}
    </ProcessContext.Provider>
  );
}

export function useProcess() {
  return useContext(ProcessContext);
}
