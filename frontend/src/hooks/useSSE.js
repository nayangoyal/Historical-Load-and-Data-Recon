import { useEffect, useRef } from 'react';

export default function useSSE(onMessage) {
  const cbRef = useRef(onMessage);
  cbRef.current = onMessage;

  useEffect(() => {
    let source;
    let retryTimer;

    function connect() {
      source = new EventSource('/logs/stream');
      source.onmessage = (e) => cbRef.current(e.data);
      source.onerror = () => {
        source.close();
        retryTimer = setTimeout(connect, 3000);
      };
    }

    connect();
    return () => {
      if (source) source.close();
      clearTimeout(retryTimer);
    };
  }, []);
}
