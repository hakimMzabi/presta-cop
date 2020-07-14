package drone

case class droneMessage(droneID : String,
                        location: String,
                        time: String,
                        violationCode: String,
                        imageID: String,
                        image: String
                       )
