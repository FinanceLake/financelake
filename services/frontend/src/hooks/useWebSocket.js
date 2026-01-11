import { useState, useEffect, useRef } from 'react';
import { io } from 'socket.io-client';

const useWebSocket = (url) => {
  const [isConnected, setIsConnected] = useState(false);
  const [lastMessage, setLastMessage] = useState(null);
  const socketRef = useRef(null);

  useEffect(() => {
    // Initialize socket connection
    socketRef.current = io(url, {
      transports: ['websocket', 'polling'],
      timeout: 20000,
    });

    // Connection events
    socketRef.current.on('connect', () => {
      setIsConnected(true);
      console.log('WebSocket connected');
    });

    socketRef.current.on('disconnect', () => {
      setIsConnected(false);
      console.log('WebSocket disconnected');
    });

    socketRef.current.on('market_data', (data) => {
      setLastMessage(data);
    });

    // Cleanup on unmount
    return () => {
      if (socketRef.current) {
        socketRef.current.disconnect();
      }
    };
  }, [url]);

  const sendMessage = (event, data) => {
    if (socketRef.current && isConnected) {
      socketRef.current.emit(event, data);
    }
  };

  return {
    isConnected,
    lastMessage,
    sendMessage
  };
};

export default useWebSocket;
