import "./index.css";
import { QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { RouterProvider } from "react-router";
import { AuthProvider } from "@/components/auth/auth-provider";
import { ThemeProvider } from "@/components/shadcn/theme-provider";
import { Toaster } from "@/components/shadcn/ui/sonner";
import { queryClient } from "@/lib/query-client";
import { BootGate } from "@/pages/auth/boot-gate";
import { router } from "@/routes";

createRoot(document.getElementById("root")!).render(
    <StrictMode>
        <ThemeProvider>
            <QueryClientProvider client={queryClient}>
                <BootGate>
                    <AuthProvider>
                        <RouterProvider router={router} />
                    </AuthProvider>
                </BootGate>
                <Toaster />
            </QueryClientProvider>
        </ThemeProvider>
    </StrictMode>,
);
