package com.edgarengine;

/**
 * Created by jinchengchen on 4/29/16.
 */
public class StreetAddress {
    private String street1 = null;
    private String street2 = null;
    private String city = null;
    private String zip = null;
    private String business_phone = null;
    private String cell_phone = null;
    private String home_phone = null;

    private StreetAddress() {}

    private StreetAddress(String street1, String street2, String city, String zip, String business_phone,
                          String cell_phone, String home_phone) {
        this.street1 = street1;
        this.street2 = street2;
        this.city = city;
        this.zip = zip;
        this.business_phone = business_phone;
        this.cell_phone = cell_phone;
        this.home_phone = home_phone;
    }

    public static Builder newBuilder() {
        return new Builder();
    }


    public String street1() {
        return street1;
    }

    public String street2() {
        return street2;
    }

    public String city() {
        return city;
    }

    public String zip() {
        return zip;
    }

    public String business_phone() {
        return business_phone;
    }

    public String cell_phone() {
        return cell_phone;
    }

    public String home_phone() {
        return home_phone;
    }

    public static class Builder {
        private String street1 = null;
        private String street2 = null;
        private String city = null;
        private String zip = null;
        private String business_phone = null;
        private String cell_phone = null;
        private String home_phone = null;

        private Builder() {}

        public Builder setStreet1(String street1) {
            this.street1 = street1;
            return this;
        }

        public Builder setStreet2(String street2) {
            this.street2 = street2;
            return this;
        }

        public Builder setCity(String city) {
            this.city = city;
            return this;
        }

        public Builder setZip(String zip) {
            this.zip = zip;
            return this;
        }

        public Builder setBusinessPhone(String business_phone) {
            this.business_phone = business_phone;
            return this;
        }

        public Builder setCellPhone(String cell_phone) {
            this.cell_phone = cell_phone;
            return this;
        }

        public Builder setHomePhone(String home_phone) {
            this.home_phone = home_phone;
            return this;
        }

        public StreetAddress build() {
            return new StreetAddress(street1, street2, city, zip, business_phone, cell_phone, home_phone);
        }
    }
}
