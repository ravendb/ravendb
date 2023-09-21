import AboutViewFloating, { AccordionItemWrapper } from "components/common/AboutView";
import FeatureAvailabilitySummaryWrapper, {FeatureAvailabilityData} from "components/common/FeatureAvailabilitySummary";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import React from "react";
import {useRavenLink} from "hooks/useRavenLink";
import {Icon} from "components/common/Icon";

export function TimeSeriesInfoHub() {
    const hasTimeSeriesRollupsAndRetention = useAppSelector(licenseSelectors.statusValue("HasTimeSeriesRollupsAndRetention"));
    const timeSeriesDocsLink = useRavenLink({ hash: "LNOMKT" });

    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasTimeSeriesRollupsAndRetention,
            },
        ],
    });

    return (
        <AboutViewFloating>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    In this view, you can add a <strong>Time Series Configuration per collection</strong> to
                    help manage the large volume of time series data that accumulates.
                </p>
                <div>
                    The following can be configured per collection:
                    <ul>
                        <li className="margin-top-xxs">
                            <strong>Rollup Policies</strong>:
                            <br />
                            =&gt; Define a Rollup Policy to aggregate time series data into smaller, summarized
                            datasets called <strong>Rollups</strong>.
                            Each entry in the Rollup time series is a summary of data from a defined interval of
                            the original time series.
                            <br />
                            =&gt; Multiple Rollup Policies can be defined, each aggregating the previous Rollup time
                            series. This reduces the granularity of the raw data and may simplify analysis.
                            <br />
                            =&gt; Rollup entries are not created for time series with entries that have more
                            than 5 values (even if a policy is defined).
                        </li>
                        <li className="margin-top-xxs">
                            <strong>Retention Periods</strong>:
                            <br/>
                            A retention period can be defined for both the raw data entries and each Rollup Policy.
                            Time series entries that exceed this time frame will be removed.
                        </li>
                        <li className="margin-top-xxs">
                            <strong>Named Values</strong>:
                            <br/>
                            Each time series entry has a specific timestamp and a set of up to 32 data values.
                            A meaningful name can be given to each value in the time series entry.
                        </li>
                    </ul>
                </div>
                <hr/>
                <div>
                    Applying the configurations:
                    <ul>
                        <li className="margin-top-xxs">
                            Ensure to set the <strong>Policy Check Frequency</strong> since the server will execute the
                            defined rollup and retention policies only at that scheduled time.
                        </li>
                        <li className="margin-top-xxs">
                            Each configuration per collection can be enabled or disabled.
                        </li>
                        <li className="margin-top-xxs">
                            A configuration applies to all time series data of all documents within the given
                            collection. The Rollups content can be viewed on the Document View.
                        </li>
                    </ul>
                </div>
                <hr/>
                <div className="small-label mb-2">useful links</div>
                <a href={timeSeriesDocsLink} target="_blank">
                    <Icon icon="newtab" /> Docs - Time Series
                </a>
            </AccordionItemWrapper>
            <FeatureAvailabilitySummaryWrapper
                isUnlimited={hasTimeSeriesRollupsAndRetention}
                data={featureAvailability}
            />
        </AboutViewFloating>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Rollups & Retention",
        featureIcon: "timeseries-settings",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true }
    }
];
