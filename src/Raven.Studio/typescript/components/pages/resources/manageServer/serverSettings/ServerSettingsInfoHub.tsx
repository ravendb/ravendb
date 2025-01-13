import { AboutViewAnchored, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";
import { Icon } from "components/common/Icon";
import { useRavenLink } from "hooks/useRavenLink";

export function ServerSettingsInfoHub() {
    const configurationOverviewDocsLink = useRavenLink({ hash: "6H3QC3" });

    return (
        <AboutViewAnchored>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    The <strong>Server Settings</strong> view displays all configuration keys for the server.
                </p>
                <p className="mb-4">
                    <ul>
                        <li className="mb-1">
                            <strong>Configuration Key</strong>:
                            <br /> The key appears &quot;Orange&quot; if a value has been customized.
                            <br /> The key appears &quot;Green&quot; if the default value is in use.
                        </li>
                        <li className="mb-1">
                            <strong>Effective Value</strong>:
                            <br /> The active value currently applied for the configuration key.
                        </li>
                        <li>
                            <strong>Origin</strong>:
                            <br /> &quot;Server&quot; - indicates the configuration key was customized.
                            <br /> &quot;Default&quot; - indicates no customized value has been set.
                        </li>
                    </ul>
                </p>
                <p>
                    This view is read-only.
                    <br /> Configuration keys with a <strong>server-wide scope</strong> can be customized either via
                    environment variables, the <i>settings.json</i> file, or command-line arguments, as explained in the{" "}
                    <a href={configurationOverviewDocsLink} target="_blank">
                        configuration overview
                    </a>
                    .
                </p>
                <p>
                    Some configuration keys can be overridden by a database-specific value,
                    <br /> which will take precedence over the server-wide configuration key for that database.
                    <br /> To modify configuration keys for a specific database go to the{" "}
                    <strong>Database Settings</strong> View.
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href={configurationOverviewDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Configuration Overview
                </a>
            </AccordionItemWrapper>
        </AboutViewAnchored>
    );
}
