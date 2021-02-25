package com.thelastpickle.tlpstress.profiles

import com.datastax.driver.core.PreparedStatement
import com.datastax.driver.core.Session
import com.thelastpickle.tlpstress.PartitionKey
import com.thelastpickle.tlpstress.StressContext
import com.thelastpickle.tlpstress.generators.*
import com.thelastpickle.tlpstress.generators.functions.FirstName
import com.thelastpickle.tlpstress.generators.functions.LastName
import com.thelastpickle.tlpstress.generators.functions.USCities
import java.util.concurrent.ThreadLocalRandom

class SecondaryIndexYugabyte : IStressProfile {

    override fun prepare(session: Session) {
        insert = session.prepare("INSERT INTO person_si (name, age, city) values (?, ?, ?)")
        select_base = session.prepare("SELECT * FROM person_si WHERE name = ?")
        select_by_city = session.prepare("SELECT * FROM person_si WHERE city = ?")
        delete_base = session.prepare("DELETE FROM person_si WHERE name = ?")


    }

    override fun schema(): List<String> = listOf("""CREATE TABLE IF NOT EXISTS person_si
                        | (name text, age int, city text, primary key(name))
			| WITH transactions = { 'enabled' : true }""".trimMargin(),

                        """CREATE INDEX IF NOT EXISTS ON person_si (city)""".trimMargin())

    override fun getRunner(context: StressContext): IStressRunner {

        return object : IStressRunner {
            var select_count = 0L

            val cities = context.registry.getGenerator("person", "city")

            override fun getNextMutation(partitionKey: PartitionKey): Operation {
                val num = ThreadLocalRandom.current().nextInt(1, 110)
                return Operation.Mutation(insert.bind(partitionKey.getText(), num, cities.getText()))
            }

            override fun getNextSelect(partitionKey: PartitionKey): Operation {
                val result = Operation.SelectStatement(select_by_city.bind(cities.getText()))
                select_count++
                return result
            }

            override fun getNextDelete(partitionKey: PartitionKey): Operation {
                return Operation.Deletion(delete_base.bind(partitionKey.getText()))
            }
        }
    }

    override fun getFieldGenerators(): Map<Field, FieldGenerator> {
        val person = FieldFactory("person")
        return mapOf(person.getField("firstname") to FirstName(),
                     person.getField("lastname") to LastName(),
                     person.getField("city") to USCities()
                    )
    }


    lateinit var insert : PreparedStatement
    lateinit var select_base : PreparedStatement
    lateinit var select_by_city : PreparedStatement
    lateinit var delete_base : PreparedStatement
}
