package no.nav.sf.pdl.nks

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.serialization.Decoder
import kotlinx.serialization.Encoder
import kotlinx.serialization.KSerializer
import kotlinx.serialization.PrimitiveDescriptor
import kotlinx.serialization.PrimitiveKind
import kotlinx.serialization.SerialDescriptor

object IsoLocalDateSerializer : LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE)
open class LocalDateSerializer(private val formatter: DateTimeFormatter) : KSerializer<LocalDate> {
    override val descriptor: SerialDescriptor = PrimitiveDescriptor("LocalDate", PrimitiveKind.STRING) // StringDescriptor.withName(“java.time.LocalDate”)
    override fun deserialize(decoder: Decoder): LocalDate {
        return LocalDate.parse(decoder.decodeString(), formatter)
    }
    override fun serialize(encoder: Encoder, value: LocalDate) {
        encoder.encodeString(value.format(formatter))
    }
}
