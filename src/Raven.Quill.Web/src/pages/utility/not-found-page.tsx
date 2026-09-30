import { Link, useLocation, useNavigate, useParams } from "react-router";
import { PageErrorState } from "@/components/data/page-error-state";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";

type NotFoundPageProps = {
    homeTo?: string;
    homeLabel?: string;
};

export function NotFoundPage({ homeTo = appRoutes.dashboard(), homeLabel = "Go to dashboard" }: NotFoundPageProps) {
    const { pathname } = useLocation();
    const navigate = useNavigate();

    return (
        <PageErrorState
            code="404"
            title="Page not found"
            description={
                <p>
                    The page <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">{pathname}</code>{" "}
                    doesn&apos;t exist or has been moved.
                </p>
            }
        >
            <Button asChild>
                <Link to={homeTo}>{homeLabel}</Link>
            </Button>
            <Button variant="outline" onClick={() => navigate(-1)}>
                Go back
            </Button>
        </PageErrorState>
    );
}

export function AppScopedNotFoundPage() {
    const { slug = "" } = useParams();

    return <NotFoundPage homeTo={appRoutes.app(slug)} homeLabel="Go to app overview" />;
}
