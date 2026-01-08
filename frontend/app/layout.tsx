import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "AssistFPL - AI-Powered FPL Assistant",
  description: "Get intelligent transfer recommendations, predictions, and insights for Fantasy Premier League",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <head>
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="" />
        <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
      </head>
      <body>
        <div className="app-container">
          <nav className="navbar">
            <div className="container flex items-center justify-between">
              <a href="/" className="logo">
                <span className="logo-icon">⚽</span>
                <span className="logo-text">AssistFPL</span>
              </a>
              <div className="nav-links">
                <a href="/" className="nav-link">Dashboard</a>
                <a href="/players" className="nav-link">Players</a>
                <a href="/chat" className="nav-link">AI Chat</a>
              </div>
            </div>
          </nav>
          <main className="main-content">
            {children}
          </main>
          <footer className="footer">
            <div className="container text-center text-muted">
              AssistFPL - AI-Powered Fantasy Premier League Assistant
            </div>
          </footer>
        </div>
      </body>
    </html>
  );
}
