import { Card, CardHeader, CardTitle, CardDescription, CardContent } from "@/components/ui/card";
import Link from "next/link";
import Image from "next/image";

export default function Home() {
  return (
    <main className="flex min-h-screen flex-col bg-gray-50">
      <header className="w-full bg-white shadow-sm border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-8 py-4">
          <div className="flex items-center">
            <Image
              src="/Databricks_Logo.png"
              alt="Databricks Logo"
              width={240}
              height={80}
              className="h-16 w-auto"
              priority
            />
          </div>
        </div>
      </header>

      <div className="flex-1 flex flex-col items-center justify-center px-24 py-16">
        <div className="z-10 w-full max-w-5xl items-center justify-between font-mono text-sm lg:flex">
          <h1 className="text-4xl font-bold text-center w-full">Invoice Processing</h1>
        </div>
        <div className="text-center mt-4 mb-16">
          <p className="text-lg text-gray-600">AI-Powered Invoice Field Extraction with Human Review</p>
        </div>

        <div className="grid grid-cols-1 gap-8 justify-center max-w-sm">
          <Link href="/document-intelligence/" className="w-full">
            <Card className="h-full transform transition-transform hover:scale-105 cursor-pointer">
              <CardHeader>
                <CardTitle>Invoice Review</CardTitle>
              </CardHeader>
              <CardContent>
                <CardDescription>
                  Upload invoices, extract fields with AI, review and correct, then save to Delta Lake.
                </CardDescription>
              </CardContent>
            </Card>
          </Link>
        </div>
      </div>
    </main>
  );
}
