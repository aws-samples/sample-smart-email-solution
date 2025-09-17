'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
export default function Login() {
  const router = useRouter();

  useEffect(() => {
    const requireLogin = process.env.NEXT_PUBLIC_REQUIRE_LOGIN === 'true';
    if (!requireLogin) {
      router.push('/');
      return;
    }
    
    const token = localStorage.getItem('authToken');
    if (token) {
      router.push('/');
    }
  }, [router]);

  const handleLogin = () => {
    const COGNITO_DOMAIN = process.env.NEXT_PUBLIC_COGNITO_DOMAIN!
    const CLIENT_ID = process.env.NEXT_PUBLIC_CLIENT_ID!
    const REDIRECT_URI = process.env.NEXT_PUBLIC_REDIRECT_URI!
    const auth_url = `https://${COGNITO_DOMAIN}/oauth2/authorize?client_id=${encodeURIComponent(CLIENT_ID)}&response_type=code&scope=${encodeURIComponent('openid')}&redirect_uri=${encodeURIComponent(REDIRECT_URI)}`
    console.log("auth_url="+auth_url)
    window.location.href = auth_url;
  };

  return (
    <main className="min-h-screen flex items-center justify-center bg-gray-100">
      <Card className="w-full max-w-md p-8 space-y-6">
        <div className="text-center">
          <h1 className="text-2xl font-bold">Nova Sonic Agentic Chatbot</h1>
        </div>
        
        <Button 
          onClick={handleLogin}
          className="w-full bg-black text-white hover:bg-neutral-800"
        >
          Sign In
        </Button>
      </Card>
    </main>
  );
}