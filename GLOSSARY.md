# Tax Engine (Flink)

PoC that consolidates tax withholdings into certificates, processing changes from one PostgreSQL database with Apache Flink and materializing the result into another PostgreSQL database.

## Language

**Merchant**:
Master-data record of a taxpayer identified by their CUIT. Each CUIT maps to exactly one merchant.
_Avoid_: comercio, client, account, establishment as the identity

**Establishment**:
Descriptive attribute of a merchant. It does not identify the merchant nor partition its certificates.
_Avoid_: sucursal, branch used as a key

**Withholding**:
A tax of the `RET_*` family withheld on a transaction. The only family that consolidates into certificates in this PoC.
_Avoid_: retención, "RET" used for a single tax instead of the family

**Perception**:
A tax of the `PER_*` family added on a transaction. Out of current scope; the taxonomy exists so it can be included later.
_Avoid_: percepción, using a specific perception tax (e.g. IIBB) as a synonym for the family

**Tax Calculation**:
Immutable fact resulting from calculating one tax over one transaction. Every calculation is new; an existing one is never corrected.
_Avoid_: tax event (when it implies mutability), "cálculo"

**Certificate**:
Document that consolidates the withholdings of one merchant and one tax over one period. Once the period closes, it is immutable.
_Avoid_: certificado, receipt, voucher

**Rate Line**:
The line of a certificate corresponding to one tax rate: total taxable base and total withheld at that rate.
_Avoid_: ítem de tasa, "certificate item"

**Certification Period**:
The time interval during which withholdings consolidate into a certificate. A closed certificate no longer admits withholdings from its period.
_Avoid_: "window" when referring to the domain concept rather than the mechanism
_Physical columns_: the target schema stores the period boundaries as `window_start` / `window_end` ([docker/postgres/target/init.sql](docker/postgres/target/init.sql)) — the spec fixes those column names and the underlying SQL is a windowed aggregation; "window" remains an avoid-term in prose.
