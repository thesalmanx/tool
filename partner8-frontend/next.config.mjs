/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'export',
  env: {
    NEXT_PUBLIC_BACKEND_URL: process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000",
  },
};

export default nextConfig;
