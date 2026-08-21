import { useState } from "react";
import { Link } from "react-router";
import { KeyRound, LineChart, LogOut, ShieldCheck, UserRound } from "lucide-react";
import { toast } from "sonner";
import { useAuth } from "@/components/auth/auth-context";
import { Button } from "@/components/shadcn/ui/button";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "@/components/shadcn/ui/dropdown-menu";
import { Spinner } from "@/components/shadcn/ui/spinner";

export function UserMenu() {
    const { logout } = useAuth();
    const [isSigningOut, setIsSigningOut] = useState(false);

    async function handleSignOut() {
        setIsSigningOut(true);

        try {
            await logout();
            // On success the auth status flips to unauthenticated and RequireAuth
            // redirects to /login, unmounting this menu — no need to reset the flag.
        } catch {
            toast.error("Couldn't sign out. Please try again.");
            setIsSigningOut(false);
        }
    }

    return (
        <DropdownMenu>
            <DropdownMenuTrigger asChild>
                <Button
                    variant="ghost"
                    size="icon"
                    aria-label="Account"
                    className="text-muted-foreground hover:text-foreground"
                >
                    <UserRound className="size-4" aria-hidden="true" />
                </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-48">
                <DropdownMenuLabel>Account</DropdownMenuLabel>
                <DropdownMenuSeparator />
                <DropdownMenuItem asChild>
                    <Link to="/usage">
                        <LineChart aria-hidden="true" />
                        Usage
                    </Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                    <Link to="/license">
                        <KeyRound aria-hidden="true" />
                        License
                    </Link>
                </DropdownMenuItem>
                <DropdownMenuItem asChild>
                    <Link to="/certificates">
                        <ShieldCheck aria-hidden="true" />
                        Certificates
                    </Link>
                </DropdownMenuItem>

                <DropdownMenuSeparator />
                <DropdownMenuItem
                    variant="destructive"
                    disabled={isSigningOut}
                    onSelect={(event) => {
                        event.preventDefault();
                        void handleSignOut();
                    }}
                >
                    {isSigningOut ? <Spinner /> : <LogOut aria-hidden="true" />}
                    Sign out
                </DropdownMenuItem>
            </DropdownMenuContent>
        </DropdownMenu>
    );
}
