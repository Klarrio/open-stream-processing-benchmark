package common.utils

import java.sql.Timestamp

import common.benchmark._

/**
 * Contains observations for testing purposes of the different stages
 */
object TestObservations {

  // Test input for join phase
  // Test output of parse phase
  // Ordered by time
  val speedObservationsAfterParsingStage = List(
    // Observations of timestamp 1489584240000
    List(
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 95.0, 95, 2, "")),
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584240000l, 52.431985, 4.64708, 78.0, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584240000l, 52.07045, 4.89938, 134.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584240000l, 52.07045, 4.89938, 124.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584240000l, 52.07045, 4.89938, 111.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584240000l, 52.07045, 4.89938, 91.0, 100, 4, ""))
    ),

    // Observations of timestamp 1489584300000
    List(
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584300000l, 52.431985, 4.64708, 91.0, 95, 2, "")),
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584300000l, 52.431985, 4.64708, 81.0, 95, 2, ""))
    ),

    // Observations of timestamp 1489584360000
    List(
      ("GEO02_R_RWSTI784", SpeedObservation("GEO02_R_RWSTI784", "lane1", 1489584360000l, 52.453393, 4.658596, 0.0, 95, 1, "")),
      ("GEO01_Z_RWSTI1231", SpeedObservation("GEO01_Z_RWSTI1231", "lane1", 1489584360000l, 50.8742, 5.81804, 0.0, 95, 1, "")),
      ("GRT02_MORO_4015_2", SpeedObservation("GRT02_MORO_4015_2", "lane1", 1489584360000l, 51.927393, 4.496434, 0.0, 95, 1, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584360000l, 52.07045, 4.89938, 132.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584360000l, 52.07045, 4.89938, 121.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584360000l, 52.07045, 4.89938, 107.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584360000l, 52.07045, 4.89938, 87.0, 100, 4, "")),

      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584360000l, 52.431985, 4.64708, 96.0, 95, 2, "")),
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584360000l, 52.431985, 4.64708, 80.0, 95, 2, ""))
    ),

    // Observations of timestamp 1489584420000
    List(
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584420000l, 52.431985, 4.64708, 82.0, 95, 2, "")),
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584420000l, 52.431985, 4.64708, 85.0, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584420000l, 52.07045, 4.89938, 135.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584420000l, 52.07045, 4.89938, 122.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584420000l, 52.07045, 4.89938, 108.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584420000l, 52.07045, 4.89938, 92.0, 100, 4, ""))
    ),

    // Observations of timestamp 1489584480000
    List(
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584480000l, 52.431985, 4.64708, 87.0, 95, 2, "")),
      ("GEO02_PNHTI532r", SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584480000l, 52.431985, 4.64708, 77.0, 95, 2, ""))
    ),

    // Observations of timestamp 1489584540000
    List(
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584540000l, 52.07045, 4.89938, 133.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584540000l, 52.07045, 4.89938, 123.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584540000l, 52.07045, 4.89938, 113.0, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584540000l, 52.07045, 4.89938, 95.0, 100, 4, ""))
    )
  )

  // Test input for join phase
  // Test output of parse phase
  // Ordered by time
  val flowObservationsAfterParsingStage = List(
    // Observations of timestamp 1489584240000
    List(
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 180, 60, 95, 2, "")),
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane2", 1489584240000l, 52.431985, 4.64708, 780, 60, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584240000l, 52.07045, 4.89938, 1740, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584240000l, 52.07045, 4.89938, 1680, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584240000l, 52.07045, 4.89938, 960, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584240000l, 52.07045, 4.89938, 1260, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584300000
    List(
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane1", 1489584300000l, 52.431985, 4.64708, 300, 60, 95, 2, "")),
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane2", 1489584300000l, 52.431985, 4.64708, 780, 60, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584300000l, 52.07045, 4.89938, 1740, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584300000l, 52.07045, 4.89938, 1680, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584300000l, 52.07045, 4.89938, 960, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584300000l, 52.07045, 4.89938, 1260, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584360000
    List(
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane1", 1489584360000l, 52.431985, 4.64708, 60, 60, 95, 2, "")),
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane2", 1489584360000l, 52.431985, 4.64708, 480, 60, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584360000l, 52.07045, 4.89938, 1800, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584360000l, 52.07045, 4.89938, 2340, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584360000l, 52.07045, 4.89938, 1620, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584360000l, 52.07045, 4.89938, 1200, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584420000
    List(
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane1", 1489584420000l, 52.431985, 4.64708, 180, 60, 95, 2, "")),
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane2", 1489584420000l, 52.431985, 4.64708, 540, 60, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584420000l, 52.07045, 4.89938, 780, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584420000l, 52.07045, 4.89938, 1740, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584420000l, 52.07045, 4.89938, 2220, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584420000l, 52.07045, 4.89938, 1560, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584480000
    List(
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane2", 1489584480000l, 52.431985, 4.64708, 900, 60, 95, 2, "")),
      ("GEO02_PNHTI532r", FlowObservation("GEO02_PNHTI532r", "lane1", 1489584480000l, 52.431985, 4.64708, 180, 60, 95, 2, "")),

      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584480000l, 52.07045, 4.89938, 840, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584480000l, 52.07045, 4.89938, 1320, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584480000l, 52.07045, 4.89938, 1800, 60, 100, 4, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584480000l, 52.07045, 4.89938, 2040, 60, 100, 4, ""))
    )
  )


  val outputWindowAfterParsingStage = List(
    // Observations of timestamp 1489584240000
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 2, 1489584240000l, 52.431985, 4.64708, 960, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 4, 1489584240000l, 52.07045, 4.89938, 5640, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584300000
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 4, 1489584300000l, 52.431985, 4.64708, 2040, 60, 95, 2, "")),
        ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 8, 1489584300000l, 52.07045, 4.89938, 11280, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584360000
    List(

      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 6, 1489584360000l, 52.431985, 4.64708, 2580, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 12, 1489584360000l, 52.07045, 4.89938, 18240, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584420000
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 8, 1489584420000l, 52.431985, 4.64708, 3300, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 16, 1489584420000l, 52.07045, 4.89938, 24540, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584480000
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 10, 1489584480000l, 52.431985, 4.64708, 4380, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 20, 1489584480000l, 52.07045, 4.89938, 30540, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584540000 - no new input but window is still sliding
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 8, 1489584480000l, 52.431985, 4.64708, 3420, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 16, 1489584480000l, 52.07045, 4.89938, 24900, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584600000 - no new input but window is still sliding
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 6, 1489584480000l, 52.431985, 4.64708, 2340, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 12, 1489584480000l, 52.07045, 4.89938, 19260, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584660000 - no new input but window is still sliding
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 4, 1489584480000l, 52.431985, 4.64708, 1800, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 8, 1489584480000l, 52.07045, 4.89938, 12300, 60, 100, 4, ""))
    ),
    // Observations of timestamp 1489584720000 - no new input but window is still sliding
    List(
      ("GEO02_PNHTI532r", AggregatableFlowObservation("GEO02_PNHTI532r", 2, 1489584480000l, 52.431985, 4.64708, 1080, 60, 95, 2, "")),
      ("RWS01_MONIBAS_0121hrr0453ra", AggregatableFlowObservation("RWS01_MONIBAS_0121hrr0453ra", 4, 1489584480000l, 52.07045, 4.89938, 6000, 60, 100, 4, ""))
    )
  )

  // Test input for aggregation phase
  // Test output of join phase
  val observationsAfterJoinStage = List(
    List(
      ("GEO02_PNHTI532r/lane1/1489584240000", (FlowObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 180, 60, 95, 2, ""), SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584240000l, 52.431985, 4.64708, 95.0, 95, 2, ""))),
      ("GEO02_PNHTI532r/lane2/1489584240000", (FlowObservation("GEO02_PNHTI532r", "lane2", 1489584240000l, 52.431985, 4.64708, 780, 60, 95, 2, ""), SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584240000l, 52.431985, 4.64708, 78.0, 95, 2, ""))),

      ("RWS01_MONIBAS_0121hrr0453ra/lane2/1489584240000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584240000l, 52.07045, 4.89938, 1740, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584240000l, 52.07045, 4.89938, 124.0, 100, 4, ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane3/1489584240000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584240000l, 52.07045, 4.89938, 1680, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584240000l, 52.07045, 4.89938, 111.0, 100, 4, ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane4/1489584240000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584240000l, 52.07045, 4.89938, 960, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584240000l, 52.07045, 4.89938, 91.0, 100, 4, ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane1/1489584240000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584240000l, 52.07045, 4.89938, 1260, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584240000l, 52.07045, 4.89938, 134.0, 100, 4, "")))
    ),
    List(
      ("GEO02_PNHTI532r/lane1/1489584300000", (FlowObservation("GEO02_PNHTI532r", "lane1", 1489584241000l, 52.431985, 4.64708, 300, 60, 95, 2, ""), SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584241000l, 52.431985, 4.64708, 91.0, 95, 2, ""))),
      ("GEO02_PNHTI532r/lane2/1489584300000", (FlowObservation("GEO02_PNHTI532r", "lane2", 1489584241000l, 52.431985, 4.64708, 780, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584241000l, 52.431985, 4.64708, 81.0, 95, 2, "")))
    ),
    List(
      ("GEO02_R_RWSTI784/lane1/1489584360000", (FlowObservation("GEO02_R_RWSTI784", "lane1", 1489584242000l, 52.453393, 4.658596, 0, 60, 95, 1, ""), SpeedObservation("GEO02_R_RWSTI784", "lane1", 1489584242000l, 52.453393, 4.658596, 0.0, 95, 1,  ""))),
      ("GEO01_Z_RWSTI1231/lane1/1489584360000", (FlowObservation("GEO01_Z_RWSTI1231", "lane1", 1489584242000l, 50.8742, 5.81804, 0, 60, 95, 1,  ""), SpeedObservation("GEO01_Z_RWSTI1231", "lane1", 1489584242000l, 50.8742, 5.81804, 0.0, 95, 1,  ""))),
      ("GRT02_MORO_4015_2/lane1/1489584360000", (FlowObservation("GRT02_MORO_4015_2", "lane1", 1489584242000l, 51.927393, 4.496434, 0, 60, 95, 1,  ""), SpeedObservation("GRT02_MORO_4015_2", "lane1", 1489584242000l, 51.927393, 4.496434, 0.0, 95, 1, ""))),

      ("GEO02_PNHTI532r/lane1/1489584360000", (FlowObservation("GEO02_PNHTI532r", "lane1", 1489584242000l, 52.431985, 4.64708, 60, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584242000l, 52.431985, 4.64708, 96.0, 95, 2, ""))),
      ("GEO02_PNHTI532r/lane2/1489584360000", (FlowObservation("GEO02_PNHTI532r", "lane2", 1489584242000l, 52.431985, 4.64708, 480, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584242000l, 52.431985, 4.64708, 80.0, 95, 2, ""))),

      ("RWS01_MONIBAS_0121hrr0453ra/lane1/1489584360000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584242000l, 52.07045, 4.89938, 1800, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584242000l, 52.07045, 4.89938, 132.0, 100, 4, ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane2/1489584360000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584242000l, 52.07045, 4.89938, 2340, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584242000l, 52.07045, 4.89938, 121.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane3/1489584360000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584242000l, 52.07045, 4.89938, 1620, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584242000l, 52.07045, 4.89938, 107.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane4/1489584360000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584242000l, 52.07045, 4.89938, 1200, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584242000l, 52.07045, 4.89938, 87.0, 100, 4,  "")))
    ),
    List(
      ("GEO02_PNHTI532r/lane1/1489584420000", (FlowObservation("GEO02_PNHTI532r", "lane1", 1489584243000l, 52.431985, 4.64708, 180, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584243000l, 52.431985, 4.64708, 82.0, 95, 2, ""))),
      ("GEO02_PNHTI532r/lane2/1489584420000", (FlowObservation("GEO02_PNHTI532r", "lane2", 1489584243000l, 52.431985, 4.64708, 540, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584243000l, 52.431985, 4.64708, 85.0, 95, 2,  ""))),

      ("RWS01_MONIBAS_0121hrr0453ra/lane4/1489584420000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584243000l, 52.07045, 4.89938, 780, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584243000l, 52.07045, 4.89938, 92.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane3/1489584420000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584243000l, 52.07045, 4.89938, 1740, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584243000l, 52.07045, 4.89938, 108.0, 100, 4, ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane2/1489584420000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584243000l, 52.07045, 4.89938, 2220, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584243000l, 52.07045, 4.89938, 122.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane1/1489584420000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584243000l, 52.07045, 4.89938, 1560, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584243000l, 52.07045, 4.89938, 135.0, 100, 4,  "")))
    ),
    List(
      ("GEO02_PNHTI532r/lane2/1489584480000", (FlowObservation("GEO02_PNHTI532r", "lane2", 1489584244000l, 52.431985, 4.64708, 900, 60, 95, 2, ""), SpeedObservation("GEO02_PNHTI532r", "lane2", 1489584244000l, 52.431985, 4.64708, 77.0, 95, 2, ""))),
      ("GEO02_PNHTI532r/lane1/1489584480000", (FlowObservation("GEO02_PNHTI532r", "lane1", 1489584244000l, 52.431985, 4.64708, 180, 60, 95, 2,  ""), SpeedObservation("GEO02_PNHTI532r", "lane1", 1489584244000l, 52.431985, 4.64708, 87.0, 95, 2, "")))
    ),
    List(
      ("RWS01_MONIBAS_0121hrr0453ra/lane4/1489584540000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584245000l, 52.07045, 4.89938, 840, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane4", 1489584245000l, 52.07045, 4.89938, 95.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane1/1489584540000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584245000l, 52.07045, 4.89938, 1320, 60, 100, 4, ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane1", 1489584245000l, 52.07045, 4.89938, 133.0, 100, 4,  ""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane3/1489584540000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584245000l, 52.07045, 4.89938, 1800, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane3", 1489584245000l, 52.07045, 4.89938, 113.0, 100, 4,""))),
      ("RWS01_MONIBAS_0121hrr0453ra/lane2/1489584540000", (FlowObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584245000l, 52.07045, 4.89938, 2040, 60, 100, 4,  ""), SpeedObservation("RWS01_MONIBAS_0121hrr0453ra", "lane2", 1489584245000l, 52.07045, 4.89938, 123.0, 100, 4, "")))
    )
  )

  // Test output of aggregation phase
  val observationsAfterAggregationStage = List(
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584240000l, 52.431985, 4.64708, 960, 60, 95, 86.5, 95, 2,  ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane2", "lane3", "lane4", "lane1"), 1489584240000l, 52.07045, 4.89938, 5640, 60, 100, 115.0, 100, 4,"")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584241000l, 52.431985, 4.64708, 1080, 60, 95, 86.0, 95, 2,  "")
    ),
    List(
      AggregatableObservation("GEO01_Z_RWSTI1231", List("lane1"), 1489584242000l, 50.8742, 5.81804, 0, 60, 95, 0.0, 95, 1,  ""),
      AggregatableObservation("GEO02_R_RWSTI784", List("lane1"), 1489584242000l, 52.453393, 4.658596, 0, 60, 95, 0.0, 95, 1,  ""),
      AggregatableObservation("GRT02_MORO_4015_2", List("lane1"), 1489584242000l, 51.927393, 4.496434, 0, 60, 95, 0.0, 95, 1,  ""),

      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584242000l, 52.431985, 4.64708, 540, 60, 95, 88.0, 95, 2, ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane1", "lane2", "lane3", "lane4"), 1489584242000l, 52.07045, 4.89938, 6960, 60, 100, 111.75, 100, 4, "")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584243000l, 52.431985, 4.64708, 720, 60, 95, 83.5, 95, 2,  ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane3", "lane2", "lane1"), 1489584243000l, 52.07045, 4.89938, 6300, 60, 100, 114.25, 100, 4,  "")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane2", "lane1"), 1489584244000l, 52.431985, 4.64708, 1080, 60, 95, 82.0, 95, 2,  "")
    ),
    List(
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane1", "lane3", "lane2"), 1489584245000l, 52.07045, 4.89938, 6000, 60, 100, 116.0, 100, 4,  "")
    )
  )


  val observationsInputRelativeChangeStage = List(
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584240000l, 52.431985, 4.64708, 960, 60, 95, 86.5, 95, 2,  ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane2", "lane3", "lane4", "lane1"), 1489584240000l, 52.07045, 4.89938, 5640, 60, 100, 115.0, 100, 4,  "")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584241000l, 52.431985, 4.64708, 1080, 60, 95, 86.0, 95, 2,  "")
    ),
    List(
      AggregatableObservation("GEO02_R_RWSTI784", List("lane1"), 1489584242000l, 52.453393, 4.658596, 0, 60, 95, 0.0, 95, 1,""),
      AggregatableObservation("GEO01_Z_RWSTI1231", List("lane1"), 1489584242000l, 50.8742, 5.81804, 0, 60, 95, 0.0, 95, 1,""),
      AggregatableObservation("GEO01_Z_RWSTI709", List("lane1"), 1489584242000l, 51.6364, 4.51124, 120, 60, 95, 142.0, 95, 1, ""),
      AggregatableObservation("GEO01_Z_RWSTI636", List("lane1"), 1489584242000l, 51.4301, 4.26685, 1080, 60, 95, 121.0, 95, 3, ""),
      AggregatableObservation("GEO02_R_RWSTI4089", List("lane2"), 1489584242000l, 52.129509, 4.394245, 180, 60, 95, 0.0, 95, 2,  ""),
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584242000l, 52.431985, 4.64708, 540, 60, 95, 88.0, 95, 2, ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane1", "lane2", "lane3", "lane4"), 1489584242000l, 52.07045, 4.89938, 6960, 60, 100, 111.75, 100, 4, "")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584243000l, 52.431985, 4.64708, 720, 60, 95, 83.5, 95, 2,  ""),
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane3", "lane2", "lane1"), 1489584243000l, 52.07045, 4.89938, 6300, 60, 100, 114.25, 100, 4, "")
    ),
    List(
      AggregatableObservation("GEO02_PNHTI532r", List("lane2", "lane1"), 1489584244000l, 52.431985, 4.64708, 1080, 60, 95, 82.0, 95, 2, "")
    ),
    List(
      AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane1", "lane3", "lane2"), 1489584245000l, 52.07045, 4.89938, 6000, 60, 100, 116.0, 100, 4, "")
    )
  )

  val observationsAfterRelativeChangeStage = List(
    List(
      RelativeChangeObservation("GEO02_PNHTI532r", 1489584240000l, AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584240000l, 52.431985, 4.64708, 960, 60, 95, 86.5, 95, 2,  ""), None, None, ""),
      RelativeChangeObservation("RWS01_MONIBAS_0121hrr0453ra",1489584240000l, AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane2", "lane3", "lane4", "lane1"), 1489584240000l, 52.07045, 4.89938, 5640, 60, 100, 115.0, 100, 4,  ""), None, None, "")
    ),
    List(
      RelativeChangeObservation("GEO02_PNHTI532r", 1489584241000l, AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584241000l, 52.431985, 4.64708, 1080, 60, 95, 86.0, 95, 2,  ""), Some(RelativeChangePercentages(0.125,-0.005780346820809248)), None, "")
    ),
    List(
      RelativeChangeObservation("GEO02_R_RWSTI784", 1489584242000l, AggregatableObservation("GEO02_R_RWSTI784", List("lane1"), 1489584242000l, 52.453393, 4.658596, 0, 60, 95, 0.0, 95, 1, ""), None, None, ""),
      RelativeChangeObservation("GEO01_Z_RWSTI1231",1489584242000l,  AggregatableObservation("GEO01_Z_RWSTI1231", List("lane1"), 1489584242000l, 50.8742, 5.81804, 0, 60, 95, 0.0, 95, 1,  ""), None, None, ""),
      RelativeChangeObservation("GEO01_Z_RWSTI709", 1489584242000l, AggregatableObservation("GEO01_Z_RWSTI709", List("lane1"), 1489584242000l, 51.6364, 4.51124, 120, 60, 95, 142.0, 95, 1,  ""), None, None, ""),
      RelativeChangeObservation("GEO01_Z_RWSTI636", 1489584242000l, AggregatableObservation("GEO01_Z_RWSTI636", List("lane1"), 1489584242000l, 51.4301, 4.26685, 1080, 60, 95, 121.0, 95, 3,  ""), None, None, ""),
      RelativeChangeObservation("GEO02_R_RWSTI4089", 1489584242000l, AggregatableObservation("GEO02_R_RWSTI4089", List("lane2"), 1489584242000l, 52.129509, 4.394245, 180, 60, 95, 0.0, 95, 2,  ""), None, None, ""),
      RelativeChangeObservation("GEO02_PNHTI532r", 1489584242000l, AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584242000l, 52.431985, 4.64708, 540, 60, 95, 88.0, 95, 2,  ""), Some(RelativeChangePercentages(-0.5,0.023255813953488372)),Some(RelativeChangePercentages(-0.4375,0.017341040462427744)), ""),
      RelativeChangeObservation("RWS01_MONIBAS_0121hrr0453ra", 1489584242000l, AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane1", "lane2", "lane3", "lane4"), 1489584242000l, 52.07045, 4.89938, 6960, 60, 100, 111.75, 100, 4,  ""), None, Some(RelativeChangePercentages(0.23404255319148937,-0.02826086956521739)), "")
    ),
    List(
      RelativeChangeObservation("GEO02_PNHTI532r", 1489584243000l, AggregatableObservation("GEO02_PNHTI532r", List("lane1", "lane2"), 1489584243000l, 52.431985, 4.64708, 720, 60, 95, 83.5, 95, 2,  ""), Some(RelativeChangePercentages(0.3333333333333333,-0.05113636363636364)),Some(RelativeChangePercentages(-0.3333333333333333,-0.029069767441860465)), ""),
      RelativeChangeObservation("RWS01_MONIBAS_0121hrr0453ra", 1489584243000l, AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane3", "lane2", "lane1"), 1489584243000l, 52.07045, 4.89938, 6300, 60, 100, 114.25, 100, 4,  ""), Some(RelativeChangePercentages(-0.09482758620689655, 0.02237136465324385)), None, "")
    ),
    List(
      RelativeChangeObservation("GEO02_PNHTI532r", 1489584244000l, AggregatableObservation("GEO02_PNHTI532r", List("lane2", "lane1"), 1489584244000l, 52.431985, 4.64708, 1080, 60, 95, 82.0, 95, 2, ""), Some(RelativeChangePercentages(0.5,-0.017964071856287425)),Some(RelativeChangePercentages(1.0,-0.06818181818181818)), "")
    ),
    List(
      RelativeChangeObservation("RWS01_MONIBAS_0121hrr0453ra", 1489584245000l, AggregatableObservation("RWS01_MONIBAS_0121hrr0453ra", List("lane4", "lane1", "lane3", "lane2"), 1489584245000l, 52.07045, 4.89938, 6000, 60, 100, 116.0, 100, 4, ""), None, Some(RelativeChangePercentages(-0.047619047619047616,0.015317286652078774)), "")
    )
  )
}