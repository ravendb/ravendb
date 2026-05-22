import "./index.css";
import { QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { AuthProvider } from "@/components/auth/auth-provider";
import { ThemeProvider } from "@/components/shadcn/theme-provider";
import { queryClient } from "@/lib/query-client";
import { router } from "@/routes";
import { RouterProvider } from "react-router";
import { Toaster } from "@/components/shadcn/ui/sonner";

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <ThemeProvider>
            <QueryClientProvider client={queryClient}>
                <AuthProvider>
                    <RouterProvider router={router} />
                    <Toaster />
                </AuthProvider>
            </QueryClientProvider>
        </ThemeProvider>
    </StrictMode>,
);
