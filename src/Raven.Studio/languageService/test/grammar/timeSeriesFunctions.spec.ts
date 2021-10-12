import { parseRql } from "../../src/parser";
import {
    ProjectFieldContext, ProjectIndividualFieldsContext,
    TimeSeriesFunctionContext
} from "../../src/generated/BaseRqlParser";


describe("TimeSeries statement parser", function () {

    it("declare timeseries", function () {
        const {
            parseTree,
            parser
        } = parseRql("declare timeseries __timeSeriesQueryFunction0(person, watch){from person.Heartrate between $p3 and $p4 load Tag as watch where (Values[0] > $p1) and (watch.Accuracy >= $p2) }from 'People' as person where person.Age > $p0 select output(person) limit $p5, $p6");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const functionBody = parseTree.functionStatment()[0];

        expect(functionBody)
            .toBeInstanceOf(TimeSeriesFunctionContext);

        const timeSeriesQuery = (functionBody as TimeSeriesFunctionContext).tsFunction()._body;

        expect(timeSeriesQuery._from._name.text)
            .toEqual("person.Heartrate");

        const between = timeSeriesQuery._range.tsBetween();
        expect(between._from.text)
            .toEqual("$p3")
        expect(between._to.text)
            .toEqual("$p4")
        
        const load = timeSeriesQuery._load.tsAlias();
        expect(load._alias_text.text)
            .toEqual("watch");
    });

    it("timeseries in projection", function () {
        const { parseTree, parser } = parseRql("from 'People' as p select timeseries(from p.Heartrate between $p1 and $p2 where (Values[0] > $p0) group by '1 Months'   select average(), max(), min()) as __timeSeriesQueryFunction0 ");

        expect(parser.numberOfSyntaxErrors)
            .toEqual(0);

        const selectRQL = parseTree.selectStatement();

        expect(selectRQL)
            .toBeInstanceOf(ProjectIndividualFieldsContext);

        const ts = (selectRQL as ProjectIndividualFieldsContext)._field;
        expect(ts)
            .toBeInstanceOf(ProjectFieldContext);
        
        const timeSeriesQuery = ts.tsProg().tsQueryBody();

        expect(timeSeriesQuery._from._name.text)
            .toEqual("p.Heartrate");

        const between = timeSeriesQuery._range.tsBetween();
        expect(between._from.text)
            .toEqual("$p1")
        expect(between._to.text)
            .toEqual("$p2")

        const groupBy = timeSeriesQuery._groupBy._name;
        expect(groupBy.text)
            .toEqual("'1 Months'");

        const select = timeSeriesQuery._select._field;
        expect(select.text)
            .toEqual("average()");
    });
});
