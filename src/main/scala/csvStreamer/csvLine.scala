package csvStreamer

case class csvLine(droneID : Option[String] = None,
                   location: String,
                   time: String,
                   violationCode: Option[String] = None,
                   imageID: Option[String] = None,
                   image: Option[String] = None
                  )


