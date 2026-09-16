import { AboutViewFloating, AccordionItemWrapper } from "components/common/AboutView";
import React from "react";

export function AddNewOngoingTaskAboutView() {
    return (
        <AboutViewFloating>
            <AccordionItemWrapper icon="about" color="info" targetId="about-view">
                <div>
                    <ul>
                        <li>
                            Choose an ongoing-task to define and add to the database.
                            <br />
                            Task types can be filtered by category or searched by name.
                        </li>
                        <li className="mt-1">
                            Available categories include:
                            <ul className="mt-1">
                                <li>AI</li>
                                <li>Replication</li>
                                <li>Backups</li>
                                <li>Subscriptions</li>
                                <li>ETL (RavenDB to external targets)</li>
                                <li>Sink (external sources to RavenDB)</li>
                            </ul>
                        </li>
                    </ul>
                </div>
            </AccordionItemWrapper>
        </AboutViewFloating>
    );
}
