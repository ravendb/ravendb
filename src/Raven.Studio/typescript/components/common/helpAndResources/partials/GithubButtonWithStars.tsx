import { LazyLoad } from "components/common/LazyLoad";
import React from "react";
import { useAsync } from "react-async-hook";
import * as yup from "yup";
import { Icon } from "components/common/Icon";

export default function GithubButtonWithStars() {
    return (
        <a
            href="https://github.com/ravendb/ravendb"
            target="_blank"
            title="Star ravendb on GitHub"
            className="bg-dark d-flex align-items-center p-1 m-0 border border-dark-subtle rounded-1 gap-1 small no-decor lh-1"
        >
            <div>
                <Icon icon="github" />
                Star
            </div>
            <div className="border-start ps-1">
                <strong>
                    <StarsCount />
                </strong>
            </div>
        </a>
    );
}

function StarsCount() {
    const asyncGetStarsCount = useAsync(async () => {
        try {
            const result = await fetch("https://api.github.com/repos/ravendb/ravendb");
            const resultJson = await result.json();
            return githubApiSchema.validateSync(resultJson);
        } catch (e) {
            console.error(e);
            throw e;
        }
    }, []);

    if (asyncGetStarsCount.status === "loading") {
        return (
            <LazyLoad active>
                <div>? ???</div>
            </LazyLoad>
        );
    }

    if (asyncGetStarsCount.status === "error") {
        return null;
    }

    return <div>{asyncGetStarsCount.result.stargazers_count.toLocaleString()}</div>;
}

const githubApiSchema = yup.object({
    stargazers_count: yup.number().required(),
});
