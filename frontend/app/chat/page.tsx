'use client';

import { useState, useRef, useEffect } from 'react';
import { api } from '../lib/api';
import styles from './page.module.css';

interface Message {
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([
    {
      role: 'assistant',
      content: "👋 Hey! I'm your FPL Assistant. Ask me anything about players, transfers, or predictions!\n\nTry:\n• \"Who are the top midfielders under £8m?\"\n• \"Tell me about Salah\"\n• \"Who should I replace Werner with?\"\n• \"Haaland vs Arsenal history\"",
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim() || loading) return;

    const userMessage: Message = {
      role: 'user',
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages(prev => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      const response = await api.chat(userMessage.content);
      const assistantMessage: Message = {
        role: 'assistant',
        content: response.response,
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, assistantMessage]);
    } catch (error) {
      const errorMessage: Message = {
        role: 'assistant',
        content: '⚠️ Sorry, I encountered an error. Make sure the API server is running on http://localhost:8000',
        timestamp: new Date(),
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="container">
      <div className={styles.chatContainer}>
        <div className={styles.chatHeader}>
          <h1>🤖 AI Assistant</h1>
          <p>Powered by machine learning and real FPL data</p>
        </div>

        <div className={styles.messagesContainer}>
          {messages.map((msg, index) => (
            <div
              key={index}
              className={`${styles.message} ${styles[msg.role]} animate-fadeIn`}
              style={{ animationDelay: `${index * 50}ms` }}
            >
              <div className={styles.messageContent}>
                <div className={styles.messageHeader}>
                  <span className={styles.role}>
                    {msg.role === 'user' ? '👤 You' : '🤖 AssistFPL'}
                  </span>
                  <span className={styles.timestamp}>
                    {msg.timestamp.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
                <div className={styles.messageText}>
                  {msg.content.split('\n').map((line, i) => (
                    <p key={i}>{line || <br />}</p>
                  ))}
                </div>
              </div>
            </div>
          ))}
          {loading && (
            <div className={`${styles.message} ${styles.assistant}`}>
              <div className={styles.messageContent}>
                <div className={styles.typing}>
                  <span></span>
                  <span></span>
                  <span></span>
                </div>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>

        <form className={styles.inputContainer} onSubmit={handleSubmit}>
          <input
            type="text"
            className={`input ${styles.chatInput}`}
            placeholder="Ask about players, transfers, predictions..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={loading}
          />
          <button
            type="submit"
            className={`btn btn-primary ${styles.sendButton}`}
            disabled={loading || !input.trim()}
          >
            {loading ? '...' : 'Send'}
          </button>
        </form>
      </div>
    </div>
  );
}
