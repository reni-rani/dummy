// Databricks notebook source
// DBTITLE 1,Use libraries from Jorge Espinosa
// MAGIC %run "Users/jorge.espinosa@nubank.com.br/ImportsScalae2"

// COMMAND ----------

// DBTITLE 1,Necessary Libraries
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// DBTITLE 1,Upload corresponding information
val file_to_be_audited = spark.table("usr.audits_data.base_anexo_y_dic_24_p_1") 
// cambiar solamente p_1 a p_2 dependiendo del paquete, las bases estan cargadas al catálogo usr.audits_data

// COMMAND ----------

// DBTITLE 1,Number of records
file_to_be_audited.count

// COMMAND ----------

// DBTITLE 1,First bunch of information
//1
val customer_surnames = spark.table("mx__contract.customers__customers")
.select($"customer__first_surname", $"customer__id")
.withColumnRenamed("customer__first_surname", "hash_surname")
val customer_surnames_pii = spark.table("mx__contract.customers__customer_first_surname_pii")
.withColumnRenamed("hash", "hash_surname")
.join(customer_surnames, Seq("hash_surname"), "left")

val customer_second_names = spark.table("mx__contract.customers__customers")
.select($"customer__second_surname", $"customer__id")
.withColumnRenamed("customer__second_surname", "hash_second_name")
val customer__second_surnames_pii = spark.table("mx__contract.customers__customer_second_surname_pii")
.withColumnRenamed("hash", "hash_second_name")
.join(customer_second_names, Seq("hash_second_name"), "left")

val customer_names = spark.table("mx__contract.customers__customers")
.select($"customer__given_names", $"customer__id")
.withColumnRenamed("customer__given_names", "hash")
val customers_given_names = spark.table("mx__contract.customers__customer_given_names_pii")
.join(customer_names, Seq("hash"), "left")
.where($"customer__id".isNotNull)
.join(customer_surnames_pii, Seq("customer__id"), "left")
.join(customer__second_surnames_pii, Seq("customer__id"), "left")
.drop($"hash")
.drop($"hash_surname")
.drop($"hash_second_name")

//2
val customer_date_of_birth = spark.table("mx__contract.customers__customer_dob_pii")
.withColumnRenamed("customer__dob","real__dob")
.withColumnRenamed("hash","customer__dob")
val customers_date_of_birth_pii = spark.table("mx__contract.customers__customers") //spark.table("mx__core.customer_current_snapshot") 
.where($"customer__id".isNotNull)
.select($"customer__id", $"customer__dob")
.join(customer_date_of_birth, Seq("customer__dob"), "left")
.drop("customer__dob")
.withColumnRenamed("real__dob", "customer__dob")

//3
val customers_country_of_birth = 
spark.table("mx__contract.customers__customers")
.select($"customer__id", $"customer__country_of_birth")
.dropDuplicates() 

//4
val customers_nationality = spark.table("mx__core.customer_current_snapshot")
.where($"customer__released_at" .isNotNull) 
.where($"customer__id".isNotNull)
.select($"customer__id", $"customer__nationality")
.dropDuplicates()

//5
val customers_activity = spark.table("mx__core.customer_current_snapshot")
.where($"customer__released_at" .isNotNull) 
.where($"customer__id".isNotNull)
.select($"customer__id",$"customer__profession")

//6
val customer_billing_address = spark.table("mx__contract.customers__customers")
.select($"customer__billing_address_city", $"customer__id", $"customer__billing_address_line_1".as("hash3"), $"customer__billing_address_line_2".as("hash4"), $"customer__billing_address_locality".as("hash5"), $"customer__billing_address_number".as("hash6"), $"customer__billing_address_postcode".as("hash7"), $"customer__billing_address_state")
.withColumnRenamed("customer__billing_address_city", "hash_customer__billing_address_city")

val customers_billing_address_pii = spark.table("mx__contract.customers__customer_billing_address_city_pii")
.withColumnRenamed("hash", "hash_customer__billing_address_city")
.join(customer_billing_address, Seq("hash_customer__billing_address_city"), "left")
.join(spark.table("mx__contract.customers__customer_billing_address_line_1_pii")
     .withColumnRenamed("hash", "hash3"), Seq("hash3"), "left")
.join(spark.table("mx__contract.customers__customer_billing_address_line_2_pii")
     .withColumnRenamed("hash", "hash4"), Seq("hash4"), "left")
.join(spark.table("mx__contract.customers__customer_billing_address_locality_pii")
     .withColumnRenamed("hash", "hash5"), Seq("hash5"), "left")
.join(spark.table("mx__contract.customers__customer_billing_address_number_pii")
     .withColumnRenamed("hash", "hash6"), Seq("hash6"), "left")
.join(spark.table("mx__contract.customers__customer_billing_address_postcode_pii")
     .withColumnRenamed("hash", "hash7"), Seq("hash7"), "left")
.drop("hash_customer__billing_address_city")
.drop("hash3")
.drop("hash4")
.drop("hash5")
.drop("hash6")
.drop("hash7")
.drop("hash8")
.withColumn("address", when($"customer__billing_address_line_1".isNotNull && $"customer__billing_address_line_1"=!="" && $"customer__billing_address_line_1"=!=" " && $"customer__billing_address_line_2".isNotNull && $"customer__billing_address_line_2"=!="" && $"customer__billing_address_line_2"=!=" ", 1).otherwise(0))
.select($"customer__id", $"address", $"customer__billing_address_line_1", $"customer__billing_address_line_2", $"customer__billing_address_locality", $"customer__billing_address_number", $"customer__billing_address_postcode", $"customer__billing_address_state", $"customer__billing_address_city")

//7
val email = spark.table("mx__contract.customers__customer_email_pii")
.withColumnRenamed("customer__email", "real__email")
.withColumnRenamed("hash", "customer__email")
val current_customers_email = spark.table("mx__contract.customers__customers") //spark.table("mx__core.credit_card_account_current_snapshot") //customers__customers
.select($"customer__id",$"customer__email")
.join(email, Seq("customer__email"), "left")
.drop("customer__email")
.withColumnRenamed("real__email", "customer__email")

//8
val customer_unhashed__rfc = spark.table("mx__contract.customers__customer_cpf_pii") //spark.table("mx__contract.customers__prospect_cpf_pii") //customers__customers
.withColumnRenamed("customer__cpf","RFC_del_Acreditado")
.withColumnRenamed("hash","customer__cpf")
val customers__rfc = spark.table("mx__contract.customers__customers") //spark.table("mx__dataset.virtual_prospects_funnel")
//.where($"released_at".isNotNull)
.join(customer_unhashed__rfc,Seq("customer__cpf"),"left")
.select($"RFC_del_Acreditado".as("customer__rfc"), $"customer__id").distinct()

//9
val customers_number = spark.table("mx__contract.customers__customers")
.select($"customer__phone", $"customer__id")
.withColumnRenamed("customer__phone", "hash")
val customers__phone = spark.table("mx__contract.customers__customer_phone_pii")
.join(customers_number, Seq("hash"), "left")
.drop($"hash")

//10
val document_type = 
spark.table("mx__contract.customers__customers")
.select($"customer__id", $"customer__documents")
.withColumn("document__id", explode($"customer__documents"))
.drop($"customer__documents")
.join(spark.table("mx__contract.customers__documents").select($"document__id", $"document__type", $"document__number".as("hash"), $"document__expiration_date"), Seq("document__id"), "left")
.where($"document__type"=!="document_type__curp")
// .drop($"document__type")
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id")).orderBy(col("document__id").desc)))
.where($"rank"===1)
.drop($"rank")
.select($"customer__id", $"document__type".as("doc_type"), $"document__expiration_date") //Yes, they can stay. The limit is Dic 6th, the migration date. Anything that expires after that date is Ok

//11
def withFirstGeoDataByCustomer(df: DataFrame): DataFrame = {
  df
  .withColumn("number_timestamp", row_number().over(Window.partitionBy($"customer__id").orderBy($"event_timestamp".asc)))
    .withColumn("total_registries", max("number_timestamp").over(Window.partitionBy($"customer__id")))
    // ***This limit and avoid to get any information******
    // .where($"number_timestamp" === 1)

    // you can avoid this using first_prospect
    .where($"customer__id".isNotNull)
}

val prospectTracking = spark.table("mx__series_contract.prospect_tracking")
  .withColumnRenamed("prospect_id", "prospect__id")

val mgmers = spark.table("mx__dataset.virtual_prospects_funnel")
  .where($"prospect__invited_by".isNotNull)
  .groupBy("prospect__invited_by")
  .agg(count($"applied_at").as("mgm_prospects"), count($"released_at").as("mgm_customers"))
  .where($"mgm_prospects" > 0)
  .select("prospect__invited_by", "mgm_prospects", "mgm_customers")

val geolocations = prospectTracking
  .join(spark.table("mx__dataset.virtual_prospects_funnel_lobby_cross_sell").as("vpf"), Seq("prospect__id"), "left")
  .transform(withFirstGeoDataByCustomer)
  // In the provided list some customer ids are not released
  .where($"released_at".isNotNull)
  //.where($"first_prospect" === false)
  .select($"customer__id", $"applied_at", $"event_timestamp".as("geo_timestamp"), $"total_registries", $"lat".as("latitude"), $"lng".as("longitude"))
  .join(mgmers.withColumnRenamed("prospect__invited_by", "customer__id"), Seq("customer__id"), "left")
  .select($"customer__id", $"geo_timestamp", $"latitude", $"longitude")
  .where($"geo_timestamp".isNotNull)
  .withColumn("number_timestamp", row_number().over(Window.partitionBy($"customer__id").orderBy($"geo_timestamp".asc)))
  .where($"number_timestamp"===1)
  .drop("number_timestamp")


//12
val customers__certifications =
spark.table("mx__contract.karma_police__client_certificates")
.select($"customer__id", $"client_certificate__id", $"client_certificate__created_at", $"client_certificate__expires_at")
.dropDuplicates()
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id")).orderBy(col("client_certificate__created_at").desc)))
.where($"rank"===1)
.select($"customer__id", $"client_certificate__id")

//13
val curps = 
spark.table("mx__contract.customers__customers")
.select($"customer__id", $"customer__documents")
.withColumn("document__id", explode($"customer__documents"))
.drop($"customer__documents")
.join(spark.table("mx__contract.customers__documents").select($"document__id", $"document__type", $"document__number".as("hash")), Seq("document__id"), "left")
.where($"document__type"==="document_type__curp")
.drop($"document__type")

val renapo_pii = spark.table("mx__contract.la_banda__renapo_datas").withColumnRenamed("renapo_data__curp", "hash")
.join(spark.table("mx__contract.la_banda__renapo_data_curp_pii"), Seq("hash"), "full_outer")
.select($"hash", $"renapo_data__curp", $"renapo_data__valid")
.where($"renapo_data__valid"===true)

val curp_renapo_temp = curps.join(renapo_pii, Seq("hash"), "left")
.select($"customer__id", $"renapo_data__curp")
.where($"renapo_data__curp".isNotNull)
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id")).orderBy(col("customer__id").asc)))
.where($"rank"===1)
.drop("rank")
.withColumn("state_curp", substring(col("renapo_data__curp"),12,2))  //to obtain 12-1=11th value and one more (2), ie. the state code into the CURP

val curp = spark.table("mx__contract.incode_client__ocr_data_process_steps")
//no tenemos customer__id
//obtener customer__id
val interview_customer_id = spark.table("mx__contract.incode_client__interviews")
val curp_customer_id = curp.join(interview_customer_id, Seq("interview__id"))
.select($"interview__customer_id".as("customer__id"), $"ocr_data_process_step__curp").distinct()
//aquí debe hacerse el ajuste de que si no está en la Renapo, entonces está en la base de Riesgos
val curp_renapo = curp_renapo_temp.join(curp_customer_id, Seq("customer__id"), "full_outer")
.withColumn("renapo_data__curp_temp", when($"renapo_data__curp".isNull or $"renapo_data__curp"==="" or $"renapo_data__curp"===" ", $"ocr_data_process_step__curp").otherwise($"renapo_data__curp"))
.drop("renapo_data__curp")
//manual cases obtained by Ops team https://docs.google.com/spreadsheets/d/1YKiSdJ-ZcmmNKS1K56pL-Swqiy-B6lS2NPUvUCg7qXg/edit?gid=144770104#gid=144770104
/*.withColumn("renapo_data__curp", when($"customer__id"==="65c3c449-60fa-42b0-8668-12a8b227dacc", "PESC960902MTCRRR05")
.when($"customer__id"==="66c6328b-e26d-411c-9d7a-d54b7e6a9d21", "PEDG050901HDFDMLA0")
.when($"customer__id"==="671c0c89-7359-433c-8ab6-a83785b7eb59", "CARS580605HTSHQN12")
.when($"customer__id"==="66837bcf-b5ed-4279-b327-8371240c687e", "GAHN980117MJCRRN09")
.when($"customer__id"==="6705adc2-6bc8-41f9-a616-c0917348dfb5", "ROPE730108MNESNV01")
.when($"customer__id"==="667f398b-85a9-482e-bad1-bee9a1582e06", "PENM490127MPLRVR03")
.when($"customer__id"==="66b3ef38-810c-4b98-9d23-f1353192fbaa", "VIAJ970706HNERLN01")
.when($"customer__id"==="667a0ef6-3967-45d7-98bf-b6b986f3f9ac", "AOSL110416HVZNNSA1")
.when($"customer__id"==="675169c9-8f7d-4ad7-a0b8-bba5a422d7fb", "RAAJ831129MMCZCS09")
.when($"customer__id"==="670f2011-a163-4475-890b-dce9fdc590b6", "CAMM880305MNEBYG04")
.when($"customer__id"==="66a93035-ec39-4563-afad-534efbaeff83", "GAAL780124MDFRRL06")
.when($"customer__id"==="65b14df0-6f2c-49a1-888b-301e1c698ed0", "HECC950301MNERNR04")
.when($"customer__id"==="6112d9a9-d03e-4094-a4f6-52ed5fe65c0c", "LIEC481210HDFNNR00")
.when($"customer__id"==="65ee95cc-031a-488f-96c9-e826ae657556", "REGD830510HMNYMM01")
.when($"customer__id"==="61b805e1-2447-4418-93c5-af349422de9e", "NOBC870222HOCLLL07")
.when($"customer__id"==="65d3b324-4a36-46c3-b034-0e62d255d617", "BAZM000607HNERMRA4")
.when($"customer__id"==="66ca3ae9-7911-44ac-a9e9-4a9cadc37a82", "MUVJ861010HPLNLR01")
.when($"customer__id"==="6712a611-fb6d-4c75-a090-86d9a66af910", "OONI920206HNERXV06")
.when($"customer__id"==="65be9d72-119d-4b6e-ae1e-410c0fbf7891", "SOXC770216HNESXR08")
.when($"customer__id"==="662d1029-6d15-4e86-97a7-ebc174f94746", "YARF830920HMCNMR08")
.when($"customer__id"==="65847ba7-d190-49a9-b634-81afe1628969", "MAOF000613MNERRRA9")
.when($"customer__id"==="65db9d09-1296-40a3-a18d-3c56daadda77", "DUHI030108MNEPRRA8")
.otherwise($"renapo_data__curp_temp")
)*/
//.select($"customer__id", $"renapo_data__curp")
.join(spark.table("usr.default.curp_missing_20250227_to_download").distinct(), Seq("customer__id"), "full_outer") //check this DB
.withColumn("renapo_data__curp", when($"renapo_data__curp_fixed".isNotNull, $"renapo_data__curp_fixed")
.otherwise($"renapo_data__curp_temp"))
.where($"renapo_data__curp".isNotNull)
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id")).orderBy(col("customer__id").asc)))
.where($"rank"===1)
.drop("rank")

//14
val customers__gender = 
spark.table("mx__contract.customers__customers")
.select($"customer__id", $"customer__gender")
.dropDuplicates()

//15
val customers__state_of_birth = 
spark.table("mx__contract.customers__customers")
.select($"customer__id", $"customer__state_of_birth")
.dropDuplicates()

//16
val customers__questionnaire_pep = 
spark.table("mx__contract.malfoy__pep_self_declareds")
.select($"pep_self_declared__customer_id".as("customer__id"), $"pep_self_declared__id", $"pep_self_declared__created_at", $"pep_self_declared__person_type")
.dropDuplicates()
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id"), col("pep_self_declared__person_type")).orderBy(col("pep_self_declared__created_at").desc)))
.where($"rank"===1)
.drop("rank")
.withColumn("two_peps", when($"pep_self_declared__person_type"==="pep_self_declared_person_type__pep" or $"pep_self_declared__person_type"==="pep_self_declared_person_type__related", 1).otherwise(0))
.groupBy($"customer__id")
.agg(sum($"two_peps").as("pep_self_declared__id"))
.where($"pep_self_declared__id"===2)

//17 //It is the same as in 12

//18 //It is the same as in 11

//19
val papers__please = spark.table("mx__contract.papers_please__attachments")
.join(spark.table("mx__contract.papers_please__verification_requests"), Seq("verification_request__id"), "inner")
.where($"attachment__type" === "signature")
.withColumn("rank", row_number().over(Window.partitionBy(col("customer__id")).orderBy(col("attachment__id").desc)))
.where($"rank"===1)
.select($"customer__id", $"attachment__type", $"attachment__id".as("customer__digital_signature"))
.drop("attachment__type")

//start of non simp
//20
val customers__ine_validation = spark.table("mx__contract.paparazzi__docs_captures")
.join(spark.table("mx__contract.paparazzi__docs_attachments"), Seq("docs_capture__id"), "left")
.where($"docs_attachment_meta__type"==="document_back" or $"docs_attachment_meta__type"==="document_front" or $"docs_attachment_meta__type"==="selfie")
.where($"docs_capture__completed_at".isNotNull)
.groupBy($"docs_capture__customer_id", $"docs_attachment_meta__type")
.agg(count("docs_capture__customer_id").as("count"))
.groupBy($"docs_capture__customer_id")
.agg(count($"docs_capture__customer_id").as("3_for_3_docs"))
.where($"3_for_3_docs"===3)
.select($"docs_capture__customer_id".as("customer__id"), $"3_for_3_docs".as("docs_capture_ine_validation"))

//21
val customers__video_selfie = spark.table("mx__contract.paparazzi__docs_captures")
.join(spark.table("mx__contract.paparazzi__docs_attachments"), Seq("docs_capture__id"), "left")
.where($"docs_attachment_meta__type"==="video_selfie")
.where($"docs_capture__completed_at".isNotNull)
.withColumn("rank", row_number().over(Window.partitionBy(col("docs_capture__customer_id")).orderBy(col("docs_capture__completed_at").desc)))
.where($"rank"===1)
.select($"docs_capture__customer_id".as("customer__id"), $"docs_capture__completed_at".as("docs_capture_video_selfie"))

//22
val customers__proof_of_address = spark.table("mx__contract.paparazzi__docs_captures")
.join(spark.table("mx__contract.paparazzi__docs_attachments"), Seq("docs_capture__id"), "left")
.where($"docs_attachment_meta__type"==="proof_of_address")
.where($"docs_capture__completed_at".isNotNull)
.withColumn("rank", row_number().over(Window.partitionBy(col("docs_capture__customer_id")).orderBy(col("docs_capture__completed_at").desc)))
.where($"rank"===1)
.select($"docs_capture__customer_id".as("customer__id"), $"docs_capture__completed_at".as("docs_capture_proof_of_address"))

//23
val customers__nachita = 
spark.table("mx__contract.nachita__transactional_profiles")
.withColumn("rank", row_number().over(Window.partitionBy(col("transactional_profile__customer_id")).orderBy(col("transactional_profile__created_at").desc)))
.where($"rank"===1)
.select($"transactional_profile__customer_id".as("customer__id"), $"transactional_profile__id", $"transactional_profile__monthly_expenses", $"transactional_profile__monthly_transactions", $"transactional_profile__payment_method", $"transactional_profile__products_usage")

//24 //It is the same as in 11

// COMMAND ----------

//database with mising curps from renapo
display(spark.table("usr.default.curp_missing_20250227_to_download")
.where($"customer__id"==="617ddcdc-7c01-4798-8964-95a1b3d0fb68")
)

// COMMAND ----------

// DBTITLE 1,Check info with Curp
//show previous run and check
display(file_to_be_audited
.join(curp_renapo, Seq("customer__id"), "left")
//.select($"customer__id", $"renapo_data__curp")
//.where($"customer__id".isin("6138d761-e8be-4b1d-b47c-f0cb968d4547", "64d3e1c1-fd03-45e5-92e8-4622307a3c9c", "65c3c449-60fa-42b0-8668-12a8b227dacc", "671c0c89-7359-433c-8ab6-a83785b7eb59", "63053851-7a42-4195-9de4-66649df41959"))
.where($"renapo_data__curp".isNull)
//.agg(count($"customer__id"), countDistinct($"customer__id"))
)//.d this dont run for me

// COMMAND ----------

// DBTITLE 1,Join Final Information
file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")


// COMMAND ----------

// DBTITLE 1,Validation
val validate = file_to_be_audited
  .join(customers_given_names, Seq("customer__id"), "left")
  .withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
  .withColumn("Tipo de Persona", lit("Persona Fisica"))
  .join(customers_activity, Seq("customer__id"), "left")
  .withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
  .join(customers_nationality, Seq("customer__id"), "left")
  .withColumnRenamed("customer__nationality", "Nacionalidad")
  .join(customers_date_of_birth_pii, Seq("customer__id"), "left")
  .withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
  .join(customers__rfc, Seq("customer__id"), "left")
  .withColumnRenamed("customer__rfc", "RFC")
  .join(curp_renapo, Seq("customer__id"), "left")
  .withColumnRenamed("renapo_data__curp", "CURP")
  .join(geolocations, Seq("customer__id"), "left")
  .withColumnRenamed("geo_timestamp", "Geolocalizacion")
  .withColumnRenamed("latitude", "Latitud")
  .withColumnRenamed("longitude", "Longitud")
  .join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
  .withColumn("Direccion del trabajo", lit("N/A"))
  .withColumn("Telefono Casa", lit("N/A"))
  .withColumn("Telefono Oficina", lit("N/A"))
  .join(customers__phone, Seq("customer__id"), "left")
  .withColumnRenamed("customer__phone", "Telefono Celular")
  .withColumn("Otro Telefono", lit("N/A"))
  .join(current_customers_email, Seq("customer__id"), "left")
  .withColumnRenamed("customer__email", "Correo electrónico")
  .drop("customer__given_names")
  .drop("customer__first_surname")
  .drop("customer__second_surname")
  .drop("state_curp")
  .drop("address")
  .withColumn("rank", row_number().over(Window.orderBy($"customer__id")))
  .where(
    $"Nombre Completo del cliente".isNull ||
    $"Numero de cliente".isNull ||
    $"Numero de cuenta".isNull ||
    $"Tipo de Persona".isNull ||
    $"Actividad, Giro o Profesion".isNull ||
    $"Nacionalidad".isNull ||
    $"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)".isNull ||
    $"RFC".isNull ||
    $"CURP".isNull ||
    $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)".isNull ||
    $"Direccion del trabajo".isNull ||
    $"Telefono Casa".isNull ||
    $"Telefono Oficina".isNull ||
    $"Telefono Celular".isNull ||
    $"Otro Telefono".isNull ||
    $"correo electrónico".isNull ||
    $"Numero y Nombre de la Sucursal".isNull ||
    $"Direccion de la Sucursal".isNull ||
    $"Tipo de Deposito o de Cuenta".isNull ||
    $"Numero de contrato de captacion".isNull ||
    $"Fecha de Apertura".isNull ||
    $"Fecha de vencimiento".isNull ||
    $"Plazo del deposito".isNull ||
    $"Forma de pago de rendimientos".isNull ||
    $"Tasa de Interes pactada".isNull ||
    $"Fecha de ultimo deposito del cliente".isNull ||
    $"Monto del utimo deposito del cliente".isNull ||
    $"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".isNull ||
    $"Saldo de Capital".isNull ||
    $"Saldo de los intereses devengados no pagados".isNull ||
    $"Saldo Total".isNull
  )

  .select($"customer__id",
$"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
  


// COMMAND ----------

// DBTITLE 1,Show validation
display(validate)
//seems Actividad & Nacionalidad have problems too, what about address?

// COMMAND ----------

// DBTITLE 1,First 500,000 package
file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id"))) //here puts at the end records with CURP, Nacionalad & Actividad nulls
.where($"rank"<=500000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">500000 and $"rank"<=1000000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">1000000 and $"rank"<=1500000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">1500000 and $"rank"<=2000000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">2000000 and $"rank"<=2500000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">2500000 and $"rank"<=3000000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">3000000 and $"rank"<=3500000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">3500000 and $"rank"<=4000000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">4000000 and $"rank"<=4500000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"CURP".asc_nulls_last, $"Nacionalidad".asc_nulls_last, $"Actividad, Giro o Profesion".asc_nulls_last, $"customer__id")))
.where($"rank">4500000 and $"rank"<=5000000)

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.d

// COMMAND ----------

// DBTITLE 1,Count all information
file_to_be_audited
.join(customers_given_names, Seq("customer__id"), "left")
.withColumn("Nombre Completo del cliente", concat($"customer__given_names", lit(" "), $"customer__first_surname", lit(" "), $"customer__second_surname"))
//.join(spark.table("mx__dataset.savings_customers_current_snapshot").select($"customer__id", $"savings_account__id"), Seq("customer__id"), "left")
.withColumn("Tipo de Persona", lit("Persona Fisica"))//Tipo de Persona    ( física o Moral) Put it as Persona Fisica
.join(customers_activity, Seq("customer__id"), "left")
.withColumnRenamed("customer__profession", "Actividad, Giro o Profesion")
.join(customers_nationality, Seq("customer__id"), "left")
.withColumnRenamed("customer__nationality", "Nacionalidad")
.join(customers_date_of_birth_pii, Seq("customer__id"), "left")
.withColumnRenamed("customer__dob", "Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)")
.join(customers__rfc, Seq("customer__id"), "left")
.withColumnRenamed("customer__rfc", "RFC")
.join(curp_renapo, Seq("customer__id"), "left")
.withColumnRenamed("renapo_data__curp", "CURP")
.join(geolocations, Seq("customer__id"), "left")
.withColumnRenamed("geo_timestamp", "Geolocalizacion")
.withColumnRenamed("latitude", "Latitud")
.withColumnRenamed("longitude", "Longitud")
.join(customers_billing_address_pii
.withColumn("Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", concat(coalesce($"customer__billing_address_line_1", lit("")), lit(", "), coalesce($"customer__billing_address_line_2", lit("")), lit(", "), coalesce($"customer__billing_address_number", lit("")), lit(", "), coalesce($"customer__billing_address_locality", lit("")), lit(", "), coalesce($"customer__billing_address_city", lit("")), lit(", "), coalesce($"customer__billing_address_state", lit("")), lit(", "), coalesce($"customer__billing_address_postcode", lit(""))))
.select($"customer__id", $"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)")
, Seq("customer__id"), "left")
.withColumn("Direccion del trabajo", lit("N/A"))
.withColumn("Telefono Casa", lit("N/A"))
.withColumn("Telefono Oficina", lit("N/A"))
.join(customers__phone, Seq("customer__id"), "left")
.withColumnRenamed("customer__phone", "Telefono Celular")
.withColumn("Otro Telefono", lit("N/A"))
.join(current_customers_email, Seq("customer__id"), "left")
.withColumnRenamed("customer__email", "Correo electrónico")
.drop("customer__given_names")
.drop("customer__first_surname")
.drop("customer__second_surname")
.drop("state_curp")
.drop("address")
.withColumn("rank", row_number().over(Window.orderBy($"customer__id")))

.select($"Nombre Completo del cliente", 
$"Numero de cliente".as("Número de cliente"), 
$"Numero de cuenta".as("Número de cuenta"), 
$"Tipo de Persona".as("Tipo de Persona ( física o Moral)"), 
$"Actividad, Giro o Profesion", 
$"Nacionalidad", 
$"Fecha de Nacimiento (Persona física) o Constitucion ( persona Moral)", 
$"RFC", 
$"CURP", 
$"Domicilio Particular (Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y codigo postal)", 
$"Direccion del trabajo".as("Dirección del trabajo ( Calle, Número exterior e interior, colonia o demarcación, ciudad, entidad federativa y  codigo postal)"), 
$"Telefono Casa", 
$"Telefono Oficina", 
$"Telefono Celular", 
$"Otro Telefono", 
$"correo electrónico", 
$"Numero y Nombre de la Sucursal".as("Número y Nombre de la Sucursal"), 
$"Direccion de la Sucursal".as("Dirección de la Sucursal"), 
$"Tipo de Deposito o de Cuenta".as("Tipo de Depósito o de Cuenta (a la vista, de ahorro, a plazo, retirables en dias preestablecidos, retirables con previo aviso, individual, solidaria, mancumunada)"), 
$"Numero de contrato de captacion".as("Número de contratos de captación"), 
$"Fecha de Apertura", 
$"Fecha de vencimiento", 
$"Plazo del deposito".as("Plazo del depósito"), 
$"Forma de pago de rendimientos".as("Forma de pago de rendimientos, en su caso (semanal, quincenal, mensual, etc)"), 
$"Tasa de Interes pactada", 
$"Fecha de ultimo deposito del cliente".as("Fecha de ultimo depósito del cliente"), 
$"Monto del utimo deposito del cliente", 
$"Porcentaje del saldo total cubierto por el seguro de depositos al mes que corresponda".as("Porcentaje del salto total cubierto por el seguro de depositos al mes que corresponda"), 
$"Saldo de Capital", 
$"Saldo de los intereses devengados no pagados".as("Saldo de los intereses devengados no pagados, en su caso"), 
$"Saldo Total")
.count

// COMMAND ----------

"Hola a todos"

// COMMAND ----------


