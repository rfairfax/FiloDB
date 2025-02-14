package filodb.query.util

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import kamon.Kamon
import kamon.testkit.InstrumentInspection.Syntax.counterInstrumentInspection

import filodb.core.query.ColumnFilter
import filodb.core.query.Filter.Equals

class HierarchicalQueryExperienceSpec extends AnyFunSpec with Matchers {

  it("getMetricColumnFilterTag should return expected column") {
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "__name__"), "_metric_") shouldEqual "__name__"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "_metric_"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "_metric_") shouldEqual "_metric_"
    HierarchicalQueryExperience.getMetricColumnFilterTag(Seq("tag1", "tag2"), "__name__") shouldEqual "__name__"
  }

  it("getNextLevelAggregatedMetricName should return expected metric name") {

    val params = IncludeAggRule("agg_2", Set("job", "instance"))

    // Case 1: Should not update if metric doesn't have the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName(
      "metric1", ":::", params.metricSuffix) shouldEqual "metric1"

    // Case 2: Should update if metric has the aggregated metric identifier
    HierarchicalQueryExperience.getNextLevelAggregatedMetricName(
      "metric1:::agg", ":::", params.metricSuffix) shouldEqual "metric1:::agg_2"
  }

  it("isParentPeriodicSeriesPlanAllowedForRawSeriesUpdateForHigherLevelAggregatedMetric return expected values") {
    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "Aggregate", "ScalarOperation")) shouldEqual true

    HierarchicalQueryExperience.isParentPeriodicSeriesPlanAllowed(
      Seq("BinaryJoin", "ScalarOperation")) shouldEqual false
  }

  it("isRangeFunctionAllowed should return expected values") {
    HierarchicalQueryExperience.isRangeFunctionAllowed("rate") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("increase") shouldEqual true
    HierarchicalQueryExperience.isRangeFunctionAllowed("sum_over_time") shouldEqual false
    HierarchicalQueryExperience.isRangeFunctionAllowed("last") shouldEqual false
  }

  it("isAggregationOperatorAllowed should return expected values") {
    HierarchicalQueryExperience.isAggregationOperatorAllowed("sum") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("min") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("max") shouldEqual true
    HierarchicalQueryExperience.isAggregationOperatorAllowed("avg") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("count") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("topk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("bottomk") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stddev") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("stdvar") shouldEqual false
    HierarchicalQueryExperience.isAggregationOperatorAllowed("quantile") shouldEqual false
  }

  it("should check if higher level aggregation is applicable with IncludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2")), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2", "tag3")), Seq("tag1", "tag2", "_ws_", "_ns_", "__name__")) shouldEqual true

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      IncludeAggRule("agg_2", Set("tag1", "tag2", "tag3")), Seq("tag3", "tag4", "_ws_", "_ns_", "__name__")) shouldEqual false
  }

  it("should check if higher level aggregation is applicable with ExcludeTags") {
    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag2")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag3")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag1", "tag2")),Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual false

    HierarchicalQueryExperience.isHigherLevelAggregationApplicable(
      ExcludeAggRule("agg_2", Set("tag3", "tag4")), Seq("tag1", "tag2", "_ws_", "_ns_", "_metric_")) shouldEqual true
  }

  it("checkAggregateQueryEligibleForHigherLevelAggregatedMetric should increment counter if metric updated") {
    val excludeRule = ExcludeAggRule("agg_2", Set("notAggTag1", "notAggTag2"))
    val params = HierarchicalQueryExperienceParams(":::", Map("agg" -> excludeRule))
    Kamon.init()
    var counter = Kamon.counter("hierarchical-query-plans-optimized")

    // CASE 1: Should update if metric have the aggregated metric identifier
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 0
    var updatedFilters = HierarchicalQueryExperience.upsertMetricColumnFilterIfHigherLevelAggregationApplicable(
      params, Seq(
        ColumnFilter("__name__", Equals("metric1:::agg")),
        ColumnFilter("_ws_", Equals("testws")),
        ColumnFilter("_ns_", Equals("testns")),
        ColumnFilter("aggTag", Equals("value"))))
    updatedFilters.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
      .shouldEqual("metric1:::agg_2")
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 1


    // CASE 2: Should not update if metric doesn't have the aggregated metric identifier
    // reset the counter
    counter = Kamon.counter("hierarchical-query-plans-optimized")
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 0
    updatedFilters = HierarchicalQueryExperience.upsertMetricColumnFilterIfHigherLevelAggregationApplicable(
      params, Seq(
        ColumnFilter("__name__", Equals("metric1:::agg")),
        ColumnFilter("_ws_", Equals("testws")),
        ColumnFilter("_ns_", Equals("testns")),
        ColumnFilter("notAggTag1", Equals("value")))) // using exclude tag, so should not optimize
    updatedFilters.filter(x => x.column == "__name__").head.filter.valuesStrings.head.asInstanceOf[String]
      .shouldEqual("metric1:::agg")
    // count should not increment
    counter.withTag("metric_ws", "testws").withTag("metric_ns", "testns").value shouldEqual 0

    Kamon.stop()
  }
}
