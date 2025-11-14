"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

import { fundApi } from "@/lib/api";
import { Loader2 } from "lucide-react";

export default function NewFundPage() {
  const router = useRouter();

  const [form, setForm] = useState({
    name: "",
    gp_name: "",
    fund_type: "",
    vintage_year: "",
  });

  const [errors, setErrors] = useState<any>({});
  const [loading, setLoading] = useState(false);

  // -----------------------
  // VALIDATION RULES
  // -----------------------
  const validate = () => {
    const temp: any = {};

    // FUND NAME
    if (!form.name.trim()) temp.name = "Fund Name is required.";

    // GP NAME → wajib diisi
    if (!form.gp_name.trim()) temp.gp_name = "GP Name is required.";

    // FUND TYPE → wajib diisi
    if (!form.fund_type.trim()) temp.fund_type = "Fund Type is required.";

    // VINTAGE YEAR → wajib dan harus angka 4 digit
    if (!form.vintage_year.trim())
      temp.vintage_year = "Vintage Year is required.";
    else if (!/^\d{4}$/.test(form.vintage_year))
      temp.vintage_year = "Vintage Year must be a 4-digit year.";

    setErrors(temp);
    return Object.keys(temp).length === 0;
  };

  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setForm({
      ...form,
      [e.target.name]: e.target.value,
    });
  };

  const handleSubmit = async (e: any) => {
    e.preventDefault();
    if (!validate()) return;

    setLoading(true);

    try {
      await fundApi.create({
        name: form.name,
        gp_name: form.gp_name,
        fund_type: form.fund_type,
        vintage_year: parseInt(form.vintage_year),
      });

      window.location.href = "/funds";
    } catch (err) {
      console.error("Failed to create fund:", err);
      alert("Failed to create fund. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="max-w-2xl mx-auto mt-10 p-6 bg-white rounded-xl shadow">
      <h1 className="text-2xl font-bold mb-6">Add New Fund</h1>

      <form onSubmit={handleSubmit} className="space-y-5">
        {/* FUND NAME */}
        <div>
          <label className="block font-medium">Fund Name *</label>
          <input
            type="text"
            name="name"
            className="w-full border p-2 rounded"
            value={form.name}
            onChange={handleChange}
          />
          {errors.name && (
            <p className="text-red-500 text-sm">{errors.name}</p>
          )}
        </div>

        {/* GP NAME */}
        <div>
          <label className="block font-medium">GP Name *</label>
          <input
            type="text"
            name="gp_name"
            className="w-full border p-2 rounded"
            value={form.gp_name}
            onChange={handleChange}
          />
          {errors.gp_name && (
            <p className="text-red-500 text-sm">{errors.gp_name}</p>
          )}
        </div>

        {/* FUND TYPE */}
        <div>
          <label className="block font-medium">Fund Type *</label>
          <input
            type="text"
            name="fund_type"
            className="w-full border p-2 rounded"
            value={form.fund_type}
            onChange={handleChange}
          />
          {errors.fund_type && (
            <p className="text-red-500 text-sm">{errors.fund_type}</p>
          )}
        </div>

        {/* VINTAGE YEAR */}
        <div>
          <label className="block font-medium">Vintage Year *</label>
          <input
            type="text"
            name="vintage_year"
            placeholder="2024"
            className="w-full border p-2 rounded"
            value={form.vintage_year}
            onChange={handleChange}
          />
          {errors.vintage_year && (
            <p className="text-red-500 text-sm">{errors.vintage_year}</p>
          )}
        </div>

        {/* SUBMIT BUTTON */}
        <button
          type="submit"
          disabled={loading}
          className="w-full bg-blue-600 text-white py-2 rounded-lg hover:bg-blue-700 disabled:bg-gray-400 flex items-center justify-center gap-2"
        >
          {loading && <Loader2 className="w-5 h-5 animate-spin" />}
          {loading ? "Saving..." : "Create Fund"}
        </button>
      </form>
    </div>
  );
}
