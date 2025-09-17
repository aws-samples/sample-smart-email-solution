'use client';

import { useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';

export default function Callback() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const requireLogin = process.env.NEXT_PUBLIC_REQUIRE_LOGIN === 'true';
    if (!requireLogin) {
      router.push('/');
      return;
    }
    
    const handleCallback = async () => {
      try {
        const code = searchParams.get('code');
        console.info('searchParams='+ searchParams)
        if (!code) throw new Error('No code parameter');
        console.info('code='+ code)
        const response = await fetch(`http://localhost:8000/callback?code=${code}`, {
          method: 'GET',
          headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) throw new Error('Auth failed');
        
        const { token } = await response.json();
        localStorage.setItem('authToken', token);
        router.push('/');
      } catch (error) {
        console.error('Auth callback error:', error);
        router.push('/login');
      }
    };

    handleCallback();
  }, [router, searchParams]);

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-100">
      <div className="text-center">
        <div className="animate-spin rounded-full h-8 w-8 border-2 border-gray-300 border-t-black mx-auto mb-4" />
        <p>Signing you in...</p>
      </div>
    </div>
  );
}